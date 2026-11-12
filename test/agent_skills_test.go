package test

import (
	"os"
	"path/filepath"
	"testing"
)

// TestAgentSkillsDirectoryHoldsOnlySkillsWrittenForThisRepo fails when a
// generic skill is vendored into .agents/skills or a skills-lock.json is
// added. The owner installs skills globally; vendored copies were removed
// twice, last in f6e2ace.
func TestAgentSkillsDirectoryHoldsOnlySkillsWrittenForThisRepo(t *testing.T) {
	repoSkills := map[string]bool{"verify-stash": true}

	if _, err := os.Stat(filepath.Join(repoRoot, "skills-lock.json")); err == nil {
		t.Error("skills-lock.json is checked in: skills are installed globally, not locked per repo")
	}
	entries, err := os.ReadDir(filepath.Join(repoRoot, ".agents", "skills"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !repoSkills[e.Name()] {
			t.Errorf(".agents/skills/%s is not a skill written for this repo: install generic skills globally, or add it to repoSkills in %s if it was written for Stash", e.Name(), "test/agent_skills_test.go")
		}
	}
}
