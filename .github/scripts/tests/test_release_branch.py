"""Check release branch updates without changing a GitHub repository."""

import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / "workflows/release-please.yml"


@pytest.mark.parametrize("status", [201, 204, 409, 403, 500])
def test_release_branch_update(status):
    steps = yaml.safe_load(WORKFLOW.read_text())["jobs"]["prepare"]["steps"]
    update = next(step for step in steps if step.get("name") == "Update release branch from main")
    checkout = next(step for step in steps if step.get("uses") == "actions/checkout@v4")
    assert steps.index(update) < steps.index(checkout)
    assert update["if"] == "steps.pr.outputs.branch != ''"
    assert update["env"]["RELEASE_BRANCH"] == "${{ steps.pr.outputs.branch }}"

    # Run the actual workflow script with the merge API responses documented by GitHub.
    harness = """
const fs = require('node:fs');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const script = fs.readFileSync(0, 'utf8');
const status = Number(process.env.MERGE_STATUS);
const result = {requests: [], failures: [], error: null};
const github = {rest: {repos: {merge: async request => {
  result.requests.push(request);
  if (status >= 400) throw Object.assign(new Error('API failure'), {status});
  return {status, data: status === 201 ? {sha: 'merged-commit'} : undefined};
}}}};
const core = {setFailed: message => result.failures.push(message)};
(async () => {
  try {
    await new AsyncFunction('github', 'context', 'core', script)(
      github, {repo: {owner: 'rimelabs', repo: 'rime-sdk'}}, core
    );
  } catch (error) {
    result.error = {message: error.message, status: error.status};
  }
  process.stdout.write(JSON.stringify(result));
})();
"""
    result = subprocess.run(
        ["node", "-e", harness],
        input=update["with"]["script"],
        env=dict(
            os.environ,
            RELEASE_BRANCH="release-please--branches--main",
            MERGE_STATUS=str(status),
        ),
        text=True,
        capture_output=True,
        check=True,
    )
    output = json.loads(result.stdout)
    assert len(output["requests"]) == 1
    request = output["requests"][0]
    assert request["owner"] == "rimelabs"
    assert request["repo"] == "rime-sdk"
    assert request["base"] == "release-please--branches--main"
    assert request["head"] == "main"
    if status == 409:
        assert output["error"] is None
        assert len(output["failures"]) == 1
        assert "Merge conflict" in output["failures"][0]
        assert "release-please--branches--main" in output["failures"][0]
    elif status >= 400:
        assert output["error"]["status"] == status
        assert not output["failures"]
    else:
        assert output["error"] is None
        assert not output["failures"]
