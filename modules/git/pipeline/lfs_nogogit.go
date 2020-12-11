// Copyright 2020 The Gitea Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// +build !gogit

package pipeline

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"code.gitea.io/gitea/modules/git"
	"code.gitea.io/gitea/modules/git/providers/native"
	"code.gitea.io/gitea/modules/git/service"
)

// LFSResult represents commits found using a provided pointer file hash
type LFSResult struct {
	Name           string
	SHA            string
	Summary        string
	When           time.Time
	ParentHashes   []service.Hash
	BranchName     string
	FullCommitName string
}

type lfsResultSlice []*LFSResult

func (a lfsResultSlice) Len() int           { return len(a) }
func (a lfsResultSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a lfsResultSlice) Less(i, j int) bool { return a[j].When.After(a[i].When) }

// FindLFSFile finds commits that contain a provided pointer file hash
func FindLFSFile(repo service.Repository, hash service.Hash) ([]*LFSResult, error) {
	resultsMap := map[string]*LFSResult{}
	results := make([]*LFSResult, 0)

	basePath := repo.Path()

	hashStr := hash.String()

	// Use rev-list to provide us with all commits in order
	revListReader, revListWriter := io.Pipe()
	defer func() {
		_ = revListWriter.Close()
		_ = revListReader.Close()
	}()

	go func() {
		stderr := strings.Builder{}
		err := git.NewCommand("rev-list", "--all").RunInDirPipeline(repo.Path(), revListWriter, &stderr)
		if err != nil {
			_ = revListWriter.CloseWithError(git.ConcatenateError(err, (&stderr).String()))
		} else {
			_ = revListWriter.Close()
		}
	}()

	// Next feed the commits in order into cat-file --batch, followed by their trees and sub trees as necessary.
	// so let's create a batch stdin and stdout
	batchStdinReader, batchStdinWriter := io.Pipe()
	batchStdoutReader, batchStdoutWriter := io.Pipe()
	defer func() {
		_ = batchStdinReader.Close()
		_ = batchStdinWriter.Close()
		_ = batchStdoutReader.Close()
		_ = batchStdoutWriter.Close()
	}()

	go func() {
		stderr := strings.Builder{}
		err := git.NewCommand("cat-file", "--batch").RunInDirFullPipeline(repo.Path(), batchStdoutWriter, &stderr, batchStdinReader)
		if err != nil {
			_ = revListWriter.CloseWithError(git.ConcatenateError(err, (&stderr).String()))
		} else {
			_ = revListWriter.Close()
		}
	}()

	// For simplicities sake we'll us a buffered reader to read from the cat-file --batch
	batchReader := bufio.NewReader(batchStdoutReader)

	// We'll use a scanner for the revList because it's simpler than a bufio.Reader
	scan := bufio.NewScanner(revListReader)
	trees := [][]byte{}
	paths := []string{}

	fnameBuf := make([]byte, 4096)
	modeBuf := make([]byte, 40)
	workingShaBuf := make([]byte, 40)

	for scan.Scan() {
		// Get the next commit ID
		commitID := scan.Bytes()

		// push the commit to the cat-file --batch process
		_, err := batchStdinWriter.Write(commitID)
		if err != nil {
			return nil, err
		}
		_, err = batchStdinWriter.Write([]byte{'\n'})
		if err != nil {
			return nil, err
		}

		var curCommit service.Commit
		curPath := ""

	commitReadingLoop:
		for {
			_, typ, size, err := native.ReadBatchLine(batchReader)
			if err != nil {
				return nil, err
			}

			switch typ {
			case "tag":
				// This shouldn't happen but if it does well just get the commit and try again
				id, err := git.ReadTagObjectID(batchReader, size)
				if err != nil {
					return nil, err
				}
				_, err = batchStdinWriter.Write([]byte(id + "\n"))
				if err != nil {
					return nil, err
				}
				continue
			case "commit":
				// Read in the commit to get its tree and in case this is one of the last used commits
				curCommit, err = native.CommitFromReader(repo, native.StringHash(string(commitID)), io.LimitReader(batchReader, int64(size)))
				if err != nil {
					return nil, err
				}

				_, err := batchStdinWriter.Write([]byte(curCommit.TreeID().String() + "\n"))
				if err != nil {
					return nil, err
				}
				curPath = ""
			case "tree":
				var n int64
				for n < size {
					mode, fname, sha, count, err := native.ParseTreeLine(batchReader, modeBuf, fnameBuf, workingShaBuf)
					if err != nil {
						return nil, err
					}
					n += int64(count)
					if bytes.Equal(sha, []byte(hashStr)) {
						result := LFSResult{
							Name:         curPath + string(fname),
							SHA:          curCommit.ID().String(),
							Summary:      strings.Split(strings.TrimSpace(curCommit.Message()), "\n")[0],
							When:         curCommit.Author().When,
							ParentHashes: curCommit.Parents(),
						}
						resultsMap[curCommit.ID().String()+":"+curPath+string(fname)] = &result
					} else if string(mode) == git.EntryModeTree.String() {
						trees = append(trees, sha)
						paths = append(paths, curPath+string(fname)+"/")
					}
				}
				if len(trees) > 0 {
					_, err := batchStdinWriter.Write(trees[len(trees)-1])
					if err != nil {
						return nil, err
					}
					_, err = batchStdinWriter.Write([]byte("\n"))
					if err != nil {
						return nil, err
					}
					curPath = paths[len(paths)-1]
					trees = trees[:len(trees)-1]
					paths = paths[:len(paths)-1]
				} else {
					break commitReadingLoop
				}
			}
		}
	}

	if err := scan.Err(); err != nil {
		return nil, err
	}

	for _, result := range resultsMap {
		hasParent := false
		for _, parentHash := range result.ParentHashes {
			if _, hasParent = resultsMap[parentHash.String()+":"+result.Name]; hasParent {
				break
			}
		}
		if !hasParent {
			results = append(results, result)
		}
	}

	sort.Sort(lfsResultSlice(results))

	// Should really use a go-git function here but name-rev is not completed and recapitulating it is not simple
	shasToNameReader, shasToNameWriter := io.Pipe()
	nameRevStdinReader, nameRevStdinWriter := io.Pipe()
	errChan := make(chan error, 1)
	wg := sync.WaitGroup{}
	wg.Add(3)

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(nameRevStdinReader)
		i := 0
		for scanner.Scan() {
			line := scanner.Text()
			if len(line) == 0 {
				continue
			}
			result := results[i]
			result.FullCommitName = line
			result.BranchName = strings.Split(line, "~")[0]
			i++
		}
	}()
	go NameRevStdin(shasToNameReader, nameRevStdinWriter, &wg, basePath)
	go func() {
		defer wg.Done()
		defer shasToNameWriter.Close()
		for _, result := range results {
			i := 0
			if i < len(result.SHA) {
				n, err := shasToNameWriter.Write([]byte(result.SHA)[i:])
				if err != nil {
					errChan <- err
					break
				}
				i += n
			}
			var err error
			n := 0
			for n < 1 {
				n, err = shasToNameWriter.Write([]byte{'\n'})
				if err != nil {
					errChan <- err
					break
				}

			}

		}
	}()

	wg.Wait()

	select {
	case err, has := <-errChan:
		if has {
			return nil, fmt.Errorf("Unable to obtain name for LFS files. Error: %w", err)
		}
	default:
	}

	return results, nil
}
