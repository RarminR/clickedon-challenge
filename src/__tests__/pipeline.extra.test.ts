import { describe, it, expect } from "vitest";
import { generate, MAX_REVISIONS } from "../lib/pipeline";

// Bonus test (see README): the gate tests cover review passing immediately and
// review never passing, but not the boundary between them. A draft approved on
// the FINAL allowed revision must count as success — this pins the
// circuit-breaker's off-by-one (`<` vs `<=`) behaviour.

describe("Bonus — revision circuit-breaker boundary", () => {
  it("succeeds when review passes exactly on the last allowed revision", async () => {
    const res = await generate({
      behavior: "ok",
      advanceToNextStage: async () => {
        /* hand-off succeeds */
      },
      reviewPasses: (attempt) => attempt === MAX_REVISIONS,
    });
    expect(res.status).toBe("ok");
    expect(res.attempts).toBe(MAX_REVISIONS);
  });
});
