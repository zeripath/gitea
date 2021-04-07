// Copyright 2019 The Gitea Authors.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package pull

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"code.gitea.io/gitea/models"
	"code.gitea.io/gitea/modules/git"
	"code.gitea.io/gitea/modules/log"

	"github.com/gobwas/glob"
)

// DownloadDiffOrPatch will write the patch for the pr to the writer
func DownloadDiffOrPatch(pr *models.PullRequest, w io.Writer, patch bool) error {
	if err := pr.LoadBaseRepo(); err != nil {
		log.Error("Unable to load base repository ID %d for pr #%d [%d]", pr.BaseRepoID, pr.Index, pr.ID)
		return err
	}

	gitRepo, err := git.OpenRepository(pr.BaseRepo.RepoPath())
	if err != nil {
		return fmt.Errorf("OpenRepository: %v", err)
	}
	defer gitRepo.Close()
	if err := gitRepo.GetDiffOrPatch(pr.MergeBase, pr.GetGitRefName(), w, patch); err != nil {
		log.Error("Unable to get patch file from %s to %s in %s Error: %v", pr.MergeBase, pr.HeadBranch, pr.BaseRepo.FullName(), err)
		return fmt.Errorf("Unable to get patch file from %s to %s in %s Error: %v", pr.MergeBase, pr.HeadBranch, pr.BaseRepo.FullName(), err)
	}
	return nil
}

var patchErrorSuffices = []string{
	": already exists in index",
	": patch does not apply",
	": already exists in working directory",
	"unrecognized input",
}

// TestPatch will test whether a simple patch will apply
func TestPatch(pr *models.PullRequest) error {
	// Clone base repo.
	tmpBasePath, err := createTemporaryRepo(pr)
	if err != nil {
		log.Error("CreateTemporaryPath: %v", err)
		return err
	}
	defer func() {
		if err := models.RemoveTemporaryPath(tmpBasePath); err != nil {
			log.Error("Merge: RemoveTemporaryPath: %s", err)
		}
	}()

	gitRepo, err := git.OpenRepository(tmpBasePath)
	if err != nil {
		return fmt.Errorf("OpenRepository: %v", err)
	}
	defer gitRepo.Close()

	// 1. update merge base
	pr.MergeBase, err = git.NewCommand("merge-base", "--", "base", "tracking").RunInDir(tmpBasePath)
	if err != nil {
		var err2 error
		pr.MergeBase, err2 = gitRepo.GetRefCommitID(git.BranchPrefix + "base")
		if err2 != nil {
			return fmt.Errorf("GetMergeBase: %v and can't find commit ID for base: %v", err, err2)
		}
	}
	pr.MergeBase = strings.TrimSpace(pr.MergeBase)

	// 2. Check for conflicts
	if conflicts, err := checkConflicts(pr, gitRepo, tmpBasePath); err != nil || conflicts || pr.Status == models.PullRequestStatusEmpty {
		return err
	}

	// 3. Check for protected files changes
	if err = checkPullFilesProtection(pr, gitRepo); err != nil {
		return fmt.Errorf("pr.CheckPullFilesProtection(): %v", err)
	}

	if len(pr.ChangedProtectedFiles) > 0 {
		log.Trace("Found %d protected files changed", len(pr.ChangedProtectedFiles))
	}

	pr.Status = models.PullRequestStatusMergeable

	return nil
}

const conflictStatus = "DD AU UD UA DU AA UU "

func checkConflicts(pr *models.PullRequest, gitRepo *git.Repository, tmpBasePath string) (bool, error) {
	// 1. preset the pr.Status as checking (this is not saved at present)
	pr.Status = models.PullRequestStatusChecking

	// 2. Now get the pull request configuration to check if we need to ignore whitespace
	prUnit, err := pr.BaseRepo.GetUnit(models.UnitTypePullRequests)
	if err != nil {
		return false, err
	}
	prConfig := prUnit.PullRequestsConfig()

	// 3. Read the base branch in to the index of the temporary repository
	if _, err := git.NewCommand("read-tree", "base").RunInDir(tmpBasePath); err != nil {
		return false, fmt.Errorf("git read-tree %s: %v", pr.BaseBranch, err)
	}

	// 4. Now use read-tree -m to shortcut if there are no conflicts
	if _, err := git.NewCommand("read-tree", "-m", "--aggressive", pr.MergeBase, "base", "tracking").RunInDir(tmpBasePath); err != nil {
		return false, fmt.Errorf("git read-tree %s: %v", pr.BaseBranch, err)
	}

	// 4b. Now check git status to see if there are any conflicted files
	statusReader, statusWriter := io.Pipe()
	defer func() {
		_ = statusReader.Close()
		_ = statusWriter.Close()
	}()

	go func() {
		stderr := &strings.Builder{}
		if err := git.NewCommand("status", "--porcelain", "-z").RunInDirPipeline(tmpBasePath, statusWriter, stderr); err != nil {
			_ = statusWriter.CloseWithError(git.ConcatenateError(err, stderr.String()))
		} else {
			_ = statusWriter.Close()
		}
	}()

	conflictFiles := []string{}
	numFiles := 0

	bufReader := bufio.NewReader(statusReader)
loop:
	for {
		line, err := bufReader.ReadString('\000')
		if err != nil {
			break loop
		}
		if len(line) < 3 {
			continue
		}
		if line[0] == 'R' || line[1] == 'R' {
			_, err = bufReader.ReadString('\000')
			if err != nil {
				break loop
			}
		}
		if line[0] == ' ' {
			continue
		}
		numFiles++
		filename := line[3 : len(line)-1]
		if strings.Contains(conflictStatus, line[0:3]) {
			conflictFiles = append(conflictFiles, filename[:len(filename)-1])
		}
	}

	// 4c. If there are no files changed this is an empty patch
	if numFiles == 0 {
		log.Debug("PullRequest[%d]: Patch is empty - ignoring", pr.ID)
		pr.Status = models.PullRequestStatusEmpty
		pr.ConflictedFiles = []string{}
		pr.ChangedProtectedFiles = []string{}
		return false, nil
	}

	// 4d. If there are no potentially conflicted files stop
	if len(conflictFiles) == 0 {
		pr.ConflictedFiles = []string{}
		return false, nil
	}

	// 5a. Prepare the diff
	diffReader, diffWriter := io.Pipe()
	defer func() {
		_ = diffReader.Close()
		_ = diffWriter.Close()
	}()

	args := make([]string, 0, len(conflictFiles)+5)
	args = append(args, "diff", "-p", pr.MergeBase, "tracking", "--")
	args = append(args, conflictFiles...)

	// 5b. Create a plain patch from head to base
	go func() {
		if err := git.NewCommand(args...).RunInDirPipeline(tmpBasePath, diffWriter, nil); err != nil {
			log.Error("Unable to get patch file from %s to %s in %s Error: %v", pr.MergeBase, pr.HeadBranch, pr.BaseRepo.FullName(), err)
			_ = diffWriter.CloseWithError(err)
			return
		}
		_ = diffWriter.Close()
	}()

	args = args[:0]

	// 5c. Prepare the arguments to apply the patch against the index
	args = append(args, "apply", "--check", "--cached")
	if prConfig.IgnoreWhitespaceConflicts {
		args = append(args, "--ignore-whitespace")
	}
	args = append(args, "-")
	pr.ConflictedFiles = make([]string, 0, 5)

	// 5d. Prep the pipe:
	//   - Here we could do the equivalent of:
	//  `git apply --check --cached patch_file > conflicts`
	//     Then iterate through the conflicts. However, that means storing all the conflicts
	//     in memory - which is very wasteful.
	//   - alternatively we can do the equivalent of:
	//  `git apply --check ... | grep ...`
	//     meaning we don't store all of the conflicts unnecessarily.
	stderrReader, stderrWriter := io.Pipe()
	defer func() {
		_ = stderrReader.Close()
		_ = stderrWriter.Close()
	}()

	// 5e. Run the apply --check command
	conflict := false
	go func() {
		if err := git.NewCommand(args...).RunInDirTimeoutEnvFullPipeline(nil, -1, tmpBasePath, nil, stderrWriter, diffReader); err != nil {
			_ = stderrWriter.CloseWithError(err)
			return
		}
		_ = stderrWriter.Close()
	}()
	const empty = "error: unrecognised input"

	const prefix = "error: patch failed:"
	const errorPrefix = "error: "

	conflictMap := map[string]bool{}

	// 5f. Now scan the output from the command
	scanner := bufio.NewScanner(stderrReader)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.EqualFold(line, empty) && !conflict {
			// ignore
		} else if strings.HasPrefix(line, prefix) {
			conflict = true
			filepath := strings.TrimSpace(strings.Split(line[len(prefix):], ":")[0])
			conflictMap[filepath] = true
		} else if strings.HasPrefix(line, errorPrefix) {
			conflict = true
			for _, suffix := range patchErrorSuffices {
				if strings.HasSuffix(line, suffix) {
					filepath := strings.TrimSpace(strings.TrimSuffix(line[len(errorPrefix):], suffix))
					if filepath != "" {
						conflictMap[filepath] = true
					}
					break
				}
			}
		}
		// only list 10 conflicted files
		if len(conflictMap) >= 10 {
			break
		}
	}
	err = scanner.Err()

	if len(conflictMap) > 0 {
		pr.ConflictedFiles = make([]string, 0, len(conflictMap))
		for key := range conflictMap {
			pr.ConflictedFiles = append(pr.ConflictedFiles, key)
		}
	}

	// 6. If there is a conflict the `git apply` command will return a non-zero error code - so there will be a positive error.
	if err != nil {
		if conflict {
			pr.Status = models.PullRequestStatusConflict
			log.Trace("Found %d files conflicted: %v", len(pr.ConflictedFiles), pr.ConflictedFiles)

			return true, nil
		}
		return false, fmt.Errorf("git apply --check: %v", err)
	}
	return false, nil
}

// CheckFileProtection check file Protection
func CheckFileProtection(oldCommitID, newCommitID string, patterns []glob.Glob, limit int, env []string, repo *git.Repository) ([]string, error) {
	// 1. If there are no patterns short-circuit and just return nil
	if len(patterns) == 0 {
		return nil, nil
	}

	// 2. Prep the pipe
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		log.Error("Unable to create os.Pipe for %s", repo.Path)
		return nil, err
	}
	defer func() {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
	}()

	changedProtectedFiles := make([]string, 0, limit)

	// 3. Run `git diff --name-only` to get the names of the changed files
	err = git.NewCommand("diff", "--name-only", oldCommitID, newCommitID).
		RunInDirTimeoutEnvFullPipelineFunc(env, -1, repo.Path,
			stdoutWriter, nil, nil,
			func(ctx context.Context, cancel context.CancelFunc) error {
				// Close the writer end of the pipe to begin processing
				_ = stdoutWriter.Close()
				defer func() {
					// Close the reader on return to terminate the git command if necessary
					_ = stdoutReader.Close()
				}()

				// Now scan the output from the command
				scanner := bufio.NewScanner(stdoutReader)
				for scanner.Scan() {
					path := strings.TrimSpace(scanner.Text())
					if len(path) == 0 {
						continue
					}
					lpath := strings.ToLower(path)
					for _, pat := range patterns {
						if pat.Match(lpath) {
							changedProtectedFiles = append(changedProtectedFiles, path)
							break
						}
					}
					if len(changedProtectedFiles) >= limit {
						break
					}
				}

				if len(changedProtectedFiles) > 0 {
					return models.ErrFilePathProtected{
						Path: changedProtectedFiles[0],
					}
				}
				return scanner.Err()
			})
	// 4. log real errors if there are any...
	if err != nil && !models.IsErrFilePathProtected(err) {
		log.Error("Unable to check file protection for commits from %s to %s in %s: %v", oldCommitID, newCommitID, repo.Path, err)
	}

	return changedProtectedFiles, err
}

// checkPullFilesProtection check if pr changed protected files and save results
func checkPullFilesProtection(pr *models.PullRequest, gitRepo *git.Repository) error {
	if err := pr.LoadProtectedBranch(); err != nil {
		return err
	}

	if pr.ProtectedBranch == nil {
		pr.ChangedProtectedFiles = nil
		return nil
	}

	var err error
	pr.ChangedProtectedFiles, err = CheckFileProtection(pr.MergeBase, "tracking", pr.ProtectedBranch.GetProtectedFilePatterns(), 10, os.Environ(), gitRepo)
	if err != nil && !models.IsErrFilePathProtected(err) {
		return err
	}
	return nil
}
