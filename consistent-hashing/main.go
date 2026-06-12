package consistenthashing

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/emirpasic/gods/utils"
)

type ConsistentHashing struct {
	sortedNodes  *redblacktree.Tree
	virtualNodes int
}

type Range struct {
	Start uint64
	End   uint64
}

func New(virtualNodes int) *ConsistentHashing {

	tree := redblacktree.NewWith(utils.UInt64Comparator)

	return &ConsistentHashing{
		virtualNodes: virtualNodes,
		sortedNodes:  tree,
	}
}

func (ch *ConsistentHashing) GetKey(key string) (string, error) {
	hashedKey := Hash(key)

	nextServer, found := ch.sortedNodes.Ceiling(hashedKey)

	if !found {
		nextServer = ch.sortedNodes.Left()

		if nextServer == nil {
			return "", fmt.Errorf("No server")
		}
	}

	return nextServer.Value.(string), nil
}

func (ch *ConsistentHashing) AddServer(server string) []Range {
	virtualNodes := ch.getVirtualNodes(server)
	affectedRanges := make([]Range, 0)

	for _, node := range virtualNodes {
		affectedRange := ch.getAffectedRange(node)
		affectedRanges = append(affectedRanges, affectedRange)
		ch.sortedNodes.Put(node, server)
	}

	return affectedRanges
}

func (ch *ConsistentHashing) RemoveServer(server string) []Range {
	virtualNodes := ch.getVirtualNodes(server)
	affectedRanges := make([]Range, 0)

	for _, node := range virtualNodes {
		affectedRange := ch.getAffectedRange(node)
		affectedRanges = append(affectedRanges, affectedRange)
		ch.sortedNodes.Remove(node)
	}

	return affectedRanges
}

func (ch *ConsistentHashing) getVirtualNodes(server string) []uint64 {
	virtualNodes := make([]uint64, ch.virtualNodes)

	for i := 0; i < ch.virtualNodes; i++ {
		virtualNode := server + "_" + strconv.Itoa(i)
		virtualNodes[i] = Hash(virtualNode)
	}

	return virtualNodes
}

func (ch *ConsistentHashing) getAffectedRange(node uint64) Range {
	prevNode, found := ch.sortedNodes.Floor(node)

	if !found {
		prevNode = ch.sortedNodes.Right()
		if prevNode == nil {
			return Range{Start: 0, End: node}
		}
	}

	return Range{
		Start: prevNode.Key.(uint64),
		End:   node,
	}
}

func Hash(key string) uint64 {
	hash := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(hash[:8])
}
