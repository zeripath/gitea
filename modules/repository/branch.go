// Copyright 2020 The Gitea Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package repository

import (
	"fmt"

	"code.gitea.io/gitea/models"
	"code.gitea.io/gitea/modules/git"
	"code.gitea.io/gitea/modules/git/common"
	"code.gitea.io/gitea/modules/git/service"
	gitservice "code.gitea.io/gitea/modules/git/service"
)

// GetBranch returns a branch by its name
func GetBranch(repo *models.Repository, branch string) (gitservice.Branch, error) {
	gitRepo, err := git.Service.OpenRepository(repo.RepoPath())
	if err != nil {
		return nil, err
	}
	defer gitRepo.Close()

	return gitRepo.GetBranch(branch)
}

// GetBranches returns all the branches of a repository
func GetBranches(repo *models.Repository) ([]service.Branch, error) {
	return common.GetBranchesByPath(repo.RepoPath())
}

// checkBranchName validates branch name with existing repository branches
func checkBranchName(repo *models.Repository, name string) error {
	gitRepo, err := git.Service.OpenRepository(repo.RepoPath())
	if err != nil {
		return err
	}
	defer gitRepo.Close()

	branches, err := GetBranches(repo)
	if err != nil {
		return err
	}

	for _, branch := range branches {
		if branch.Name() == name {
			return models.ErrBranchAlreadyExists{
				BranchName: branch.Name(),
			}
		} else if (len(branch.Name()) < len(name) && branch.Name()+"/" == name[0:len(branch.Name())+1]) ||
			(len(branch.Name()) > len(name) && name+"/" == branch.Name()[0:len(name)+1]) {
			return models.ErrBranchNameConflict{
				BranchName: branch.Name(),
			}
		}
	}

	if _, err := gitRepo.GetTag(name); err == nil {
		return models.ErrTagAlreadyExists{
			TagName: name,
		}
	}

	return nil
}

// CreateNewBranch creates a new repository branch
func CreateNewBranch(doer *models.User, repo *models.Repository, oldBranchName, branchName string) (err error) {
	// Check if branch name can be used
	if err := checkBranchName(repo, branchName); err != nil {
		return err
	}

	if !git.IsBranchExist(repo.RepoPath(), oldBranchName) {
		return models.ErrBranchDoesNotExist{
			BranchName: oldBranchName,
		}
	}

	if err := git.Push(repo.RepoPath(), git.PushOptions{
		Remote: repo.RepoPath(),
		Branch: fmt.Sprintf("%s:%s%s", oldBranchName, git.BranchPrefix, branchName),
		Env:    models.PushingEnvironment(doer, repo),
	}); err != nil {
		if git.IsErrPushOutOfDate(err) || git.IsErrPushRejected(err) {
			return err
		}
		return fmt.Errorf("Push: %v", err)
	}

	return nil
}

// CreateNewBranchFromCommit creates a new repository branch
func CreateNewBranchFromCommit(doer *models.User, repo *models.Repository, commit, branchName string) (err error) {
	// Check if branch name can be used
	if err := checkBranchName(repo, branchName); err != nil {
		return err
	}

	if err := git.Push(repo.RepoPath(), git.PushOptions{
		Remote: repo.RepoPath(),
		Branch: fmt.Sprintf("%s:%s%s", commit, git.BranchPrefix, branchName),
		Env:    models.PushingEnvironment(doer, repo),
	}); err != nil {
		if git.IsErrPushOutOfDate(err) || git.IsErrPushRejected(err) {
			return err
		}
		return fmt.Errorf("Push: %v", err)
	}

	return nil
}
