package compile

import (
	"sds_use/bloomfilter/z_pb/plan"
	"sds_use/bloomfilter/z_vm/engine"
)

// Scope is the output of the compile process.
// Each sql will be compiled to one or more execution unit scopes.
type Scope struct {
	NodeInfo   engine.Node
	DataSource *Source
}

// ParallelRun try to execute the scope in parallel way.
// NOTE: This is the entry point of runtime filter which uses bloom filter.
func (s *Scope) ParallelRun(c *Compile, remote bool) error {
	//....

	var err error
	err = s.handleRuntimeFilter(c)
	if err != nil {
		return err
	}

	//....
	return nil
}

func (s *Scope) handleRuntimeFilter(c *Compile) error {
	var inExprList []*plan.Expr
	if s.NodeInfo.NeedExpandRanges {
		if s.DataSource.node == nil {
			panic("can not expand ranges on remote pipeline!")
		}
		newExprList := inExprList
		if len(s.DataSource.node.BlockFilterList) > 0 {
			newExprList = append(newExprList, s.DataSource.node.BlockFilterList...)
		}
		ranges, err := c.expandRanges(s.DataSource.node, s.NodeInfo.Rel, newExprList)
		if err != nil {
			return err
		}
		s.NodeInfo.Data = append(s.NodeInfo.Data, ranges.GetAllBytes()...)
		s.NodeInfo.NeedExpandRanges = false
	}

	return nil
}
