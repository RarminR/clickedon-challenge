import { extractJson } from "./extract-json";
import {
  mockStream,
  type MockBehavior,
  type MockState,
  type TransientError,
} from "./anthropic-mock";

export interface GenerateInput {
  /** Drives the mock streaming client (see anthropic-mock.ts). */
  behavior: MockBehavior;
  /** Hands the finished draft to the next pipeline stage. May reject. */
  advanceToNextStage: () => Promise<void>;
  /** Returns true once the draft passes review. Scripted by callers/tests. */
  reviewPasses: (attempt: number) => boolean;
}

export interface GenerateResult {
  status: "ok" | "error";
  attempts: number;
}

/** Maximum revision cycles after the initial review (attempt 0). */
const MAX_REVISIONS = 3;

/** One initial stream request plus up to two retries. */
const MAX_STREAM_ATTEMPTS = 3;

/** Base delay for exponential backoff on transient errors (jitter omitted at this scale). */
const BACKOFF_BASE_MS = 50;

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

/** Rate limits and upstream server errors are worth retrying; anything else is not. */
function isTransient(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const status = (err as TransientError).status;
  return status === 429 || (typeof status === "number" && status >= 500);
}

/**
 * Streams the draft and parses its fenced JSON payload.
 *
 * - Transient upstream errors (429/5xx) are retried with exponential backoff.
 * - Truncated or malformed payloads (a dropped stream loses the closing fence)
 *   are retried immediately — a content problem, not load shedding.
 * - Non-transient errors fail fast; once retries are exhausted, the last
 *   underlying error is thrown so the caller can surface it.
 */
async function streamParsedDraft(
  behavior: MockBehavior,
  state: MockState,
): Promise<unknown> {
  let lastError: unknown = new Error("Stream produced no usable draft");
  for (
    let streamAttempt = 1;
    streamAttempt <= MAX_STREAM_ATTEMPTS;
    streamAttempt += 1
  ) {
    let text: string;
    try {
      text = await mockStream(behavior, state);
    } catch (err) {
      if (!isTransient(err)) throw err;
      lastError = err;
      if (streamAttempt < MAX_STREAM_ATTEMPTS) {
        await sleep(BACKOFF_BASE_MS * 2 ** (streamAttempt - 1));
      }
      continue;
    }
    try {
      return extractJson(text);
    } catch (err) {
      lastError = err;
    }
  }
  throw lastError;
}

/**
 * Runs one content-generation pass: stream a draft, extract it, revise until it
 * passes review, then hand off to the next stage.
 *
 * Every failure path resolves to `{ status: "error" }` rather than throwing or
 * being swallowed, so a stalled pipeline can never masquerade as a healthy one.
 */
export async function generate(input: GenerateInput): Promise<GenerateResult> {
  const state: MockState = { calls: 0 };

  // Validate that a complete draft can be streamed and parsed. This stripped
  // pipeline has no downstream consumer for the parsed value itself.
  try {
    await streamParsedDraft(input.behavior, state);
  } catch {
    return { status: "error", attempts: 0 };
  }

  // Review the initial draft (attempt 0), then allow up to MAX_REVISIONS
  // revision cycles. `attempts` reports how many revisions were needed.
  let attempts = 0;
  let approved = input.reviewPasses(attempts);
  while (!approved && attempts < MAX_REVISIONS) {
    attempts += 1;
    approved = input.reviewPasses(attempts);
  }
  if (!approved) {
    return { status: "error", attempts };
  }

  // The hand-off is part of this stage's contract: await it and surface
  // failure instead of firing and forgetting.
  try {
    await input.advanceToNextStage();
  } catch {
    return { status: "error", attempts };
  }

  return { status: "ok", attempts };
}

export { MAX_REVISIONS };
