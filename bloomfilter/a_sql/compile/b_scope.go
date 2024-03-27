package compile

import (
	"context"
	"sds_use/bloomfilter/d_objectio"
	"sds_use/bloomfilter/z_pb/plan"
	"sds_use/hyperloglog/d_containers/batch"
)

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

// ParallelRun try to execute the scope in parallel way.
func (s *Scope) ParallelRun(c *Compile, remote bool) error {
	var rds []engine.Reader

	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)
	if s.IsJoin {
		return s.JoinRun(c)
	}
	if s.IsLoad {
		return s.LoadRun(c)
	}
	if s.DataSource == nil {
		return s.MergeRun(c)
	}

	var err error
	err = s.handleRuntimeFilter(c)
	if err != nil {
		return err
	}

	numCpu := goruntime.NumCPU()
	var mcpu int

	switch {
	case remote:
		if len(s.DataSource.OrderBy) > 0 {
			panic("ordered scan can't run on remote CN!")
		}
		ctx := c.ctx
		if util.TableIsClusterTable(s.DataSource.TableDef.GetTableType()) {
			ctx = defines.AttachAccountId(ctx, catalog.System_Account)

		}
		if s.DataSource.AccountId != nil {
			ctx = defines.AttachAccountId(ctx, uint32(s.DataSource.AccountId.GetTenantId()))
		}
		blkSlice := objectio.BlockInfoSlice(s.NodeInfo.Data)
		mcpu = DeterminRuntimeDOP(numCpu, blkSlice.Len())
		rds, err = c.e.NewBlockReader(ctx, mcpu, s.DataSource.Timestamp, s.DataSource.FilterExpr,
			s.NodeInfo.Data, s.DataSource.TableDef, c.proc)
		if err != nil {
			return err
		}
		s.NodeInfo.Data = nil

	case s.NodeInfo.Rel != nil:
		switch s.NodeInfo.Rel.GetEngineType() {
		case engine.Disttae:
			blkSlice := objectio.BlockInfoSlice(s.NodeInfo.Data)
			mcpu = DeterminRuntimeDOP(numCpu, blkSlice.Len())
		case engine.Memory:
			idSlice := memoryengine.ShardIdSlice(s.NodeInfo.Data)
			mcpu = DeterminRuntimeDOP(numCpu, idSlice.Len())
		default:
			mcpu = 1
		}
		if len(s.DataSource.OrderBy) > 0 {
			// ordered scan must run on only one parallel!
			mcpu = 1
		}
		if rds, err = s.NodeInfo.Rel.NewReader(c.ctx, mcpu, s.DataSource.FilterExpr, s.NodeInfo.Data, len(s.DataSource.OrderBy) > 0); err != nil {
			return err
		}
		s.NodeInfo.Data = nil

	// FIXME:: s.NodeInfo.Rel == nil, partition table?
	default:
		var db engine.Database
		var rel engine.Relation

		ctx := c.ctx
		if util.TableIsClusterTable(s.DataSource.TableDef.GetTableType()) {
			ctx = defines.AttachAccountId(ctx, catalog.System_Account)
		}
		db, err = c.e.Database(ctx, s.DataSource.SchemaName, s.Proc.TxnOperator)
		if err != nil {
			return err
		}
		rel, err = db.Relation(ctx, s.DataSource.RelationName, c.proc)
		if err != nil {
			var e error // avoid contamination of error messages
			db, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, s.Proc.TxnOperator)
			if e != nil {
				return e
			}
			rel, e = db.Relation(c.ctx, engine.GetTempTableName(s.DataSource.SchemaName, s.DataSource.RelationName), c.proc)
			if e != nil {
				return err
			}
		}
		switch rel.GetEngineType() {
		case engine.Disttae:
			blkSlice := objectio.BlockInfoSlice(s.NodeInfo.Data)
			mcpu = DeterminRuntimeDOP(numCpu, blkSlice.Len())
		case engine.Memory:
			idSlice := memoryengine.ShardIdSlice(s.NodeInfo.Data)
			mcpu = DeterminRuntimeDOP(numCpu, idSlice.Len())
		default:
			mcpu = 1
		}
		if len(s.DataSource.OrderBy) > 0 {
			// ordered scan must run on only one parallel!
			mcpu = 1
		}
		if rel.GetEngineType() == engine.Memory ||
			s.DataSource.PartitionRelationNames == nil {
			mainRds, err := rel.NewReader(
				ctx,
				mcpu,
				s.DataSource.FilterExpr,
				s.NodeInfo.Data,
				len(s.DataSource.OrderBy) > 0)
			if err != nil {
				return err
			}
			rds = append(rds, mainRds...)
		} else {
			// handle partition table.
			blkArray := objectio.BlockInfoSlice(s.NodeInfo.Data)
			dirtyRanges := make(map[int]objectio.BlockInfoSlice, 0)
			cleanRanges := make(objectio.BlockInfoSlice, 0, blkArray.Len())
			ranges := objectio.BlockInfoSlice(blkArray.Slice(1, blkArray.Len()))
			for i := 0; i < ranges.Len(); i++ {
				blkInfo := ranges.Get(i)
				if !blkInfo.CanRemote {
					if _, ok := dirtyRanges[blkInfo.PartitionNum]; !ok {
						newRanges := make(objectio.BlockInfoSlice, 0, objectio.BlockInfoSize)
						newRanges = append(newRanges, objectio.EmptyBlockInfoBytes...)
						dirtyRanges[blkInfo.PartitionNum] = newRanges
					}
					dirtyRanges[blkInfo.PartitionNum] = append(dirtyRanges[blkInfo.PartitionNum], ranges.GetBytes(i)...)
					continue
				}
				cleanRanges = append(cleanRanges, ranges.GetBytes(i)...)
			}

			if len(cleanRanges) > 0 {
				// create readers for reading clean blocks from main table.
				mainRds, err := rel.NewReader(
					ctx,
					mcpu,
					s.DataSource.FilterExpr,
					cleanRanges,
					len(s.DataSource.OrderBy) > 0)
				if err != nil {
					return err
				}
				rds = append(rds, mainRds...)

			}
			// create readers for reading dirty blocks from partition table.
			for num, relName := range s.DataSource.PartitionRelationNames {
				subrel, err := db.Relation(c.ctx, relName, c.proc)
				if err != nil {
					return err
				}
				memRds, err := subrel.NewReader(c.ctx, mcpu, s.DataSource.FilterExpr, dirtyRanges[num], len(s.DataSource.OrderBy) > 0)
				if err != nil {
					return err
				}
				rds = append(rds, memRds...)
			}
		}
		s.NodeInfo.Data = nil
	}

	if len(rds) != mcpu {
		newRds := make([]engine.Reader, 0, mcpu)
		step := len(rds) / mcpu
		for i := 0; i < len(rds); i += step {
			m := disttae.NewMergeReader(rds[i : i+step])
			newRds = append(newRds, m)
		}
		rds = newRds
	}

	if mcpu == 1 {
		s.Magic = Normal
		s.DataSource.R = rds[0] // rds's length is equal to mcpu so it is safe to do it
		s.DataSource.R.SetOrderBy(s.DataSource.OrderBy)
		return s.Run(c)
	}

	if len(s.DataSource.OrderBy) > 0 {
		panic("ordered scan must run on only one parallel!")
	}
	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = newScope(Normal)
		ss[i].NodeInfo = s.NodeInfo
		ss[i].DataSource = &Source{
			R:            rds[i],
			SchemaName:   s.DataSource.SchemaName,
			RelationName: s.DataSource.RelationName,
			Attributes:   s.DataSource.Attributes,
			AccountId:    s.DataSource.AccountId,
		}
		ss[i].Proc = process.NewWithAnalyze(s.Proc, c.ctx, 0, c.anal.Nodes())
	}
	newScope, err := newParallelScope(c, s, ss)
	if err != nil {
		ReleaseScopes(ss)
		return err
	}
	newScope.SetContextRecursively(s.Proc.Ctx)
	return newScope.MergeRun(c)
}

func (s *Scope) JoinRun(c *Compile) error {
	mcpu := s.NodeInfo.Mcpu
	if mcpu <= 1 { // no need to parallel
		buildScope := c.newJoinBuildScope(s, nil)
		s.PreScopes = append(s.PreScopes, buildScope)
		if s.BuildIdx > 1 {
			probeScope := c.newJoinProbeScope(s, nil)
			s.PreScopes = append(s.PreScopes, probeScope)
		}
		return s.MergeRun(c)
	}

	isRight := s.isRight()

	chp := s.PreScopes
	for i := range chp {
		chp[i].IsEnd = true
	}

	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = newScope(Merge)
		ss[i].NodeInfo = s.NodeInfo
		ss[i].Proc = process.NewWithAnalyze(s.Proc, s.Proc.Ctx, 2, c.anal.Nodes())
		ss[i].Proc.Reg.MergeReceivers[1].Ch = make(chan *batch.Batch, 10)
	}
	probe_scope, build_scope := c.newJoinProbeScope(s, ss), c.newJoinBuildScope(s, ss)
	var err error
	s, err = newParallelScope(c, s, ss)
	if err != nil {
		ReleaseScopes(ss)
		return err
	}

	if isRight {
		channel := make(chan *bitmap.Bitmap, mcpu)
		for i := range s.PreScopes {
			switch arg := s.PreScopes[i].Instructions[0].Arg.(type) {
			case *right.Argument:
				arg.Channel = channel
				arg.NumCPU = uint64(mcpu)
				if i == 0 {
					arg.IsMerger = true
				}

			case *rightsemi.Argument:
				arg.Channel = channel
				arg.NumCPU = uint64(mcpu)
				if i == 0 {
					arg.IsMerger = true
				}

			case *rightanti.Argument:
				arg.Channel = channel
				arg.NumCPU = uint64(mcpu)
				if i == 0 {
					arg.IsMerger = true
				}
			}
		}
	}
	s.PreScopes = append(s.PreScopes, chp...)
	s.PreScopes = append(s.PreScopes, build_scope)
	s.PreScopes = append(s.PreScopes, probe_scope)

	return s.MergeRun(c)
}
