package git

import (
	"bufio"
	"bytes"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/emirpasic/gods/trees/binaryheap"
)

// GitLogRawRepo opens git log --raw in the provided repo and returns a stdin pipe, a stdout reader and cancel function
func GitLogRawRepo(repository, head, treepath string, paths ...string) (*bufio.Reader, func()) {
	// We often want to feed the commits in order into cat-file --batch, followed by their trees and sub trees as necessary.
	// so let's create a batch stdin and stdout
	stdoutReader, stdoutWriter := io.Pipe()
	cancel := func() {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
	}

	args := make([]string, 0, 8+len(paths))
	args = append(args, "log", "--raw", "--format=%H %ct %P", "--parents", "--no-abbrev", "--no-renames", "-t", "-z", head, "--")
	if len(paths) < 70 {
		if treepath != "" {
			args = append(args, treepath)
			for _, pth := range paths {
				if pth != "" {
					args = append(args, path.Join(treepath, pth))
				}
			}
		} else {
			for _, pth := range paths {
				if pth != "" {
					args = append(args, pth)
				}
			}
		}
	}

	go func() {
		stderr := strings.Builder{}
		err := NewCommand(args...).RunInDirFullPipeline(repository, stdoutWriter, &stderr, nil)
		if err != nil {
			_ = stdoutWriter.CloseWithError(ConcatenateError(err, (&stderr).String()))
		} else {
			_ = stdoutWriter.Close()
		}
	}()

	// For simplicities sake we'll us a buffered reader to read from the cat-file --batch
	bufReader := bufio.NewReaderSize(stdoutReader, 32*1024)

	return bufReader, cancel
}

type GitLogRawRepoParser struct {
	treepath string
	paths    []string
	next     []byte
	buffull  bool
	rd       *bufio.Reader
	cancel   func()
}

func NewGitLogRawRepoParser(repository, head, treepath string, paths ...string) *GitLogRawRepoParser {
	rd, cancel := GitLogRawRepo(repository, head, treepath, paths...)
	return &GitLogRawRepoParser{
		treepath: treepath,
		paths:    paths,
		rd:       rd,
		cancel:   cancel,
	}
}

type GitLogCommit struct {
	CommitID  string
	Timestamp int64
	ParentIDs []string
	Paths     []bool
}

func (g *GitLogRawRepoParser) Next(treepath string, paths2ids map[string]int, ids []byte) (*GitLogCommit, error) {
	var err error
	if g.next == nil || len(g.next) == 0 {
		g.buffull = false
		g.next, err = g.rd.ReadSlice('\x00')
		if err != nil {
			if err == bufio.ErrBufferFull {
				g.buffull = true
			} else if err == io.EOF {
				return nil, nil
			} else {
				return nil, err
			}
		}
	}

	ret := GitLogCommit{}
	// Assume we're at a start
	// Our "line" must look like: <commitid> SP <timestamp> SP (<parent> SP) * NUL
	ret.CommitID = string(g.next[0:40])
	idx := bytes.IndexByte(g.next[41:], ' ')
	ret.Timestamp, _ = strconv.ParseInt(string(g.next[41:41+idx]), 10, 64)
	g.next = g.next[41+idx+1:]
	parents := string(g.next)
	if g.buffull {
		more, err := g.rd.ReadString('\x00')
		if err != nil {
			return nil, err
		}
		parents += more
	}
	parents = parents[:len(parents)-1]
	ret.ParentIDs = strings.Split(parents, " ")

	// now read the next "line"
	g.buffull = false
	g.next, err = g.rd.ReadSlice('\x00')
	if err != nil {
		if err == bufio.ErrBufferFull {
			g.buffull = true
		} else if err != io.EOF {
			return nil, err
		}
	}

	if err == io.EOF || g.next[0] != '\n' {
		return &ret, nil
	}

	// Ok we have some changes.
	// This line will look like: NL COLON <omode 6> SP <nmode 6> SP <osha 40> SP <nsha 40> SP <modifier 1> NUL
	// followed by <fname> NUL
	//
	// Subsequent lines will not have the NL - so drop it here - g.bufffull must also be false at this point too.
	g.next = g.next[1:]

	var nshaHolder [40]byte
	fnameBuf := make([]byte, 4096)

diffloop:
	for {
		if err == io.EOF || g.next[0] != ':' {
			return &ret, nil
		}
		// we literally only care about nsha here - which is bytes 46:86
		copy(nshaHolder[:], g.next[56:96])
		g.next, err = g.rd.ReadSlice('\x00')
		copy(fnameBuf, g.next)
		if len(fnameBuf) < len(g.next) {
			fnameBuf = append(fnameBuf, g.next[len(fnameBuf):]...)
		} else {
			fnameBuf = fnameBuf[:len(g.next)]
		}
		if err != nil {
			if err != bufio.ErrBufferFull {
				return nil, err
			}
			more, err := g.rd.ReadBytes('\x00')
			if err != nil {
				return nil, err
			}
			fnameBuf = append(fnameBuf, more...)
		}

		// read the next line
		g.buffull = false
		g.next, err = g.rd.ReadSlice('\x00')
		if err != nil {
			if err == bufio.ErrBufferFull {
				g.buffull = true
			} else if err != io.EOF {
				return nil, err
			}
		}

		if treepath != "" {
			if !bytes.HasPrefix(fnameBuf, []byte(treepath)) {
				fnameBuf = fnameBuf[:cap(fnameBuf)]
				continue diffloop
			}
		}
		fnameBuf = fnameBuf[len(treepath) : len(fnameBuf)-1]
		if len(fnameBuf) > 0 {
			if len(treepath) > 0 {
				if fnameBuf[0] != '/' || bytes.IndexByte(fnameBuf[1:], '/') >= 0 {
					fnameBuf = fnameBuf[:cap(fnameBuf)]
					continue diffloop
				}
				fnameBuf = fnameBuf[1:]
			} else if bytes.IndexByte(fnameBuf, '/') >= 0 {
				fnameBuf = fnameBuf[:cap(fnameBuf)]
				continue diffloop
			}
		}

		idx, ok := paths2ids[string(fnameBuf)]
		if !ok {
			fnameBuf = fnameBuf[:cap(fnameBuf)]
			continue diffloop
		}
		if bytes.Equal(ids[40*idx:40*(idx+1)], []byte{'\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}) {
			copy(ids[40*idx:40*(idx+1)], nshaHolder[:])
			if ret.Paths == nil {
				ret.Paths = make([]bool, len(ids)/40)
			}
			ret.Paths[idx] = true
		} else if bytes.Equal(nshaHolder[:], ids[40*idx:40*(idx+1)]) {
			if ret.Paths == nil {
				ret.Paths = make([]bool, len(ids)/40)
			}
			ret.Paths[idx] = true
		}
	}
}

func (g *GitLogRawRepoParser) Close() {
	g.cancel()
}

func WalkGitLog(repo *Repository, head *Commit, treepath string, paths ...string) (map[string]string, error) {
	// We do a tree traversal with nodes sorted by commit time
	heap := binaryheap.NewWith(func(a, b interface{}) int {
		if a.(*GitLogCommit).Timestamp < b.(*GitLogCommit).Timestamp {
			return 1
		}
		return -1
	})

	tree, err := head.SubTree(treepath)
	if err != nil {
		return nil, err
	}

	entries, err := tree.ListEntries()
	if err != nil {
		return nil, err
	}

	if len(paths) == 0 {
		paths = make([]string, 0, len(entries)+1)
		paths = append(paths, "")
		for _, entry := range entries {
			paths = append(paths, entry.Name())
		}
	}

	ids := make([]byte, 40*len(paths))
	path2idx := map[string]int{}

	for i := range paths {
		path2idx[paths[i]] = i
	}

	// for _, entry := range entries {
	// 	i := path2idx[entry.Name()]
	// 	copy(ids[40*i:40*(i+1)], []byte(entry.ID.String()))
	// }

	g := NewGitLogRawRepoParser(repo.Path, head.ID.String(), treepath, paths...)
	defer g.Close()

	coms := map[string]*GitLogCommit{}
	seen := map[string]bool{}
	results := map[string]string{}
	remaining := len(paths)
	nextRestart := (len(paths) * 2) / 3
	if nextRestart > 70 {
		nextRestart = 70
	}

	com, err := g.Next(treepath, path2idx, ids)
	if err != nil {
		g.Close()
		return nil, err
	}
	heap.Push(com)
heaploop:
	for {
		currentInterface, ok := heap.Pop()
		if !ok {
			break heaploop
		}
		current := currentInterface.(*GitLogCommit)
		if current.Paths != nil {
			for path, i := range path2idx {
				if _, ok := results[path]; !ok && current.Paths[i] {
					results[path] = current.CommitID
					delete(path2idx, path)
					remaining--
					if _, ok = results[""]; !ok {
						results[""] = current.CommitID
						delete(path2idx, "")
						remaining--
					}
				}
			}
		}

		if remaining <= 0 {
			break heaploop
		} else if remaining < nextRestart {
			heap.Clear()
			coms = map[string]*GitLogCommit{}
			seen = map[string]bool{}
			g.Close()
			remainingPaths := make([]string, 0, len(paths))
			for _, pth := range paths {
				if _, ok := path2idx[pth]; ok {
					remainingPaths = append(remainingPaths, pth)
				}
			}
			g = NewGitLogRawRepoParser(repo.Path, head.ID.String(), treepath, remainingPaths...)
			com, err := g.Next(treepath, path2idx, ids)
			if err != nil {
				g.Close()
				return nil, err
			}
			heap.Push(com)
			nextRestart = (remaining * 2) / 3
			continue heaploop
		}

		for _, parent := range current.ParentIDs {
			if parent == "" {
				continue
			}
			parentCom, ok := coms[parent]
			if !ok {
			getparentloop:
				for {
					parentCom, err = g.Next(treepath, path2idx, ids)
					if err != nil {
						g.Close()
						return nil, err
					}
					if parentCom == nil {
						// missing parent
						log("Missing parent for %s %s %s", repo.Path, com.CommitID, parent)
						continue getparentloop
					}
					coms[parentCom.CommitID] = parentCom
					if parentCom.CommitID == parent {
						break getparentloop
					}
				}
			}
			if !seen[parent] {
				heap.Push(parentCom)
				seen[parent] = true
			}
		}
	}
	g.Close()
	return results, nil
}
