package compile

import (
	"context"
	"sds_use/bloomfilter/z_pb/plan"
	"sds_use/bloomfilter/z_vm/engine"
)

type Compile struct {
}

func (c *Compile) expandRanges(n *plan.Node, rel engine.Relation, blockFilterList []*plan.Expr) (engine.Ranges, error) {
	// ...

	ranges, err := rel.Ranges(context.Background(), blockFilterList)
	if err != nil {
		return nil, err
	}

	// ...
	return ranges, nil
}
