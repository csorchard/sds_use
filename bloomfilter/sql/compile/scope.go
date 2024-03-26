package compile

func (s *Scope) handleRuntimeFilter(c *Compile) error {
	var err error
	var inExprList []*plan.Expr
	exprs := make([]*plan.Expr, 0, len(s.DataSource.RuntimeFilterSpecs))
	filters := make([]*pbpipeline.RuntimeFilter, 0, len(exprs))

	if len(s.DataSource.RuntimeFilterSpecs) > 0 {
		for _, spec := range s.DataSource.RuntimeFilterSpecs {
			c.lock.RLock()
			receiver, ok := c.runtimeFilterReceiverMap[spec.Tag]
			c.lock.RUnlock()
			if !ok {
				continue
			}

		FOR_LOOP:
			for i := 0; i < receiver.size; i++ {
				select {
				case <-s.Proc.Ctx.Done():
					return nil

				case filter := <-receiver.ch:
					switch filter.Typ {
					case pbpipeline.RuntimeFilter_PASS:
						continue

					case pbpipeline.RuntimeFilter_DROP:
						exprs = nil
						// FIXME: Should give an empty "Data" and then early return
						s.NodeInfo.Data = nil
						s.NodeInfo.NeedExpandRanges = false
						s.DataSource.FilterExpr = plan2.MakeFalseExpr()
						break FOR_LOOP

					case pbpipeline.RuntimeFilter_IN:
						inExpr := plan2.MakeInExpr(c.ctx, spec.Expr, filter.Card, filter.Data, spec.MatchPrefix)
						inExprList = append(inExprList, inExpr)

						// TODO: implement BETWEEN expression
					}
					exprs = append(exprs, spec.Expr)
					filters = append(filters, filter)
				}
			}
		}
	}

	for i := range inExprList {
		fn := inExprList[i].GetF()
		col := fn.Args[0].GetCol()
		if col == nil {
			panic("only support col in runtime filter's left child!")
		}

		newExpr := plan2.DeepCopyExpr(inExprList[i])
		//put expr in reader
		newExprList := []*plan.Expr{newExpr}
		if s.DataSource.FilterExpr != nil {
			newExprList = append(newExprList, s.DataSource.FilterExpr)
		}
		s.DataSource.FilterExpr = colexec.RewriteFilterExprList(newExprList)

		isFilterOnPK := s.DataSource.TableDef.Pkey != nil && col.Name == s.DataSource.TableDef.Pkey.PkeyColName
		if !isFilterOnPK {
			// put expr in filter instruction
			ins := s.Instructions[0]
			arg, ok := ins.Arg.(*restrict.Argument)
			if !ok {
				panic("missing instruction for runtime filter!")
			}
			newExprList := []*plan.Expr{newExpr}
			if arg.E != nil {
				newExprList = append(newExprList, arg.E)
			}
			arg.E = colexec.RewriteFilterExprList(newExprList)
		}
	}

	if s.NodeInfo.NeedExpandRanges {
		if s.DataSource.node == nil {
			panic("can not expand ranges on remote pipeline!")
		}
		newExprList := plan2.DeepCopyExprList(inExprList)
		if len(s.DataSource.node.BlockFilterList) > 0 {
			newExprList = append(newExprList, s.DataSource.node.BlockFilterList...)
		}
		ranges, err := c.expandRanges(s.DataSource.node, s.NodeInfo.Rel, newExprList)
		if err != nil {
			return err
		}
		s.NodeInfo.Data = append(s.NodeInfo.Data, ranges.GetAllBytes()...)
		s.NodeInfo.NeedExpandRanges = false
	} else if len(inExprList) > 0 {
		s.NodeInfo.Data, err = ApplyRuntimeFilters(c.ctx, s.Proc, s.DataSource.TableDef, s.NodeInfo.Data, exprs, filters)
		if err != nil {
			return err
		}
	}
	return nil
}
