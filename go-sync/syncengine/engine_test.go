package syncengine

import (
	"testing"
)

func TestVersionVectorCompare(t *testing.T) {
	// 1. Equal vectors
	vv1 := VersionVector{"A": 5, "B": 3}
	vv2 := VersionVector{"A": 5, "B": 3}
	if vv1.Compare(vv2) != Equal {
		t.Errorf("Expected Equal, got %v", vv1.Compare(vv2))
	}

	// 2. Newer vector (A has incremented counter)
	vv3 := VersionVector{"A": 6, "B": 3}
	if vv3.Compare(vv1) != Newer {
		t.Errorf("Expected Newer, got %v", vv3.Compare(vv1))
	}
	if vv1.Compare(vv3) != Older {
		t.Errorf("Expected Older, got %v", vv1.Compare(vv3))
	}

	// 3. Concurrent vectors (A has a higher counter, but B has a higher counter)
	vv4 := VersionVector{"A": 6, "B": 2}
	vv5 := VersionVector{"A": 5, "B": 4}
	if vv4.Compare(vv5) != Concurrent {
		t.Errorf("Expected Concurrent, got %v", vv4.Compare(vv5))
	}
}

func TestVersionVectorMerge(t *testing.T) {
	vv1 := VersionVector{"A": 5, "B": 2}
	vv2 := VersionVector{"B": 4, "C": 1}

	vv1.Merge(vv2)

	expected := VersionVector{"A": 5, "B": 4, "C": 1}
	for node, count := range expected {
		if vv1[node] != count {
			t.Errorf("Node %s: expected %d, got %d", node, count, vv1[node])
		}
	}
}
