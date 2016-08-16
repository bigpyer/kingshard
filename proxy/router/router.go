// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package router

import (
	"fmt"
	"strings"

	"github.com/flike/kingshard/config"
	"github.com/flike/kingshard/core/errors"
	"github.com/flike/kingshard/core/golog"
	"github.com/flike/kingshard/sqlparser"
)

var (
	DefaultRuleType   = "default"
	HashRuleType      = "hash"
	RangeRuleType     = "range"
	DateYearRuleType  = "date_year"
	DateMonthRuleType = "date_month"
	DateDayRuleType   = "date_day"
	MinMonthDaysCount = 28
	MaxMonthDaysCount = 31
	MonthsCount       = 12
)

type Rule struct {
	DB    string
	Table string
	Key   string

	Type           string
	Nodes          []string
	SubTableIndexs []int       //SubTableIndexs store all the index of sharding sub-table
	TableToNode    map[int]int //key is table index, and value is node index
	Shard          Shard
}

type Router struct {
	DB          string
	Rules       map[string]*Rule //key is <table name>
	DefaultRule *Rule
	Nodes       []string //just for human saw
}

func NewDefaultRule(db string, node string) *Rule {
	var r *Rule = &Rule{
		DB:          db,
		Type:        DefaultRuleType,
		Nodes:       []string{node},
		Shard:       new(DefaultShard),
		TableToNode: nil,
	}
	return r
}

func (r *Rule) FindNode(key interface{}) (string, error) {
	tableIndex, err := r.Shard.FindForKey(key)
	if err != nil {
		return "", err
	}
	nodeIndex := r.TableToNode[tableIndex]
	return r.Nodes[nodeIndex], nil
}

func (r *Rule) FindNodeIndex(key interface{}) (int, error) {
	tableIndex, err := r.Shard.FindForKey(key)
	if err != nil {
		return -1, err
	}
	return r.TableToNode[tableIndex], nil
}

func (r *Rule) FindTableIndex(key interface{}) (int, error) {
	return r.Shard.FindForKey(key)
}

//UpdateExprs is the expression after set
func (r *Rule) checkUpdateExprs(exprs sqlparser.UpdateExprs) error {
	if r.Type == DefaultRuleType {
		return nil
	} else if len(r.Nodes) == 1 {
		return nil
	}

	for _, e := range exprs {
		if string(e.Name.Name) == r.Key {
			return errors.ErrUpdateKey
		}
	}
	return nil
}

//build router according to the config file
func NewRouter(schemaConfig *config.SchemaConfig) (*Router, error) {
	/* 检查分表的default node是否包含在nodes节点中 */
	if !includeNode(schemaConfig.Nodes, schemaConfig.Default) {
		return nil, fmt.Errorf("default node[%s] not in the nodes list.",
			schemaConfig.Default)
	}

	rt := new(Router)
	rt.DB = schemaConfig.DB                                        //对应schema中的db
	rt.Nodes = schemaConfig.Nodes                                  //对应schema中的nodes
	rt.Rules = make(map[string]*Rule, len(schemaConfig.ShardRule)) //对应schema中的shard
	rt.DefaultRule = NewDefaultRule(rt.DB, schemaConfig.Default)   //对应schema中的default

	/* 遍历shard,检查配置信息 */
	for _, shard := range schemaConfig.ShardRule {
		for _, node := range shard.Nodes {
			/* 检查shard中的节点是否包含在schema中的节点内 */
			if !includeNode(rt.Nodes, node) {
				return nil, fmt.Errorf("shard table[%s] node[%s] not in the schema.nodes list:[%s].",
					shard.Table, node, strings.Join(shard.Nodes, ","))
			}
		}
		rule, err := parseRule(rt.DB, &shard)
		if err != nil {
			return nil, err
		}

		/* DefaultRuleType='default' */
		if rule.Type == DefaultRuleType {
			return nil, fmt.Errorf("[default-rule] duplicate, must only one.")
		} else {
			if _, ok := rt.Rules[rule.Table]; ok {
				return nil, fmt.Errorf("table %s rule in %s duplicate", rule.Table, rule.DB)
			}
			/* 以表名称为key，加载对应的分表规则 */
			rt.Rules[rule.Table] = rule
		}
	}
	return rt, nil
}

func (r *Router) GetRule(table string) *Rule {
	arry := strings.Split(table, ".")
	if len(arry) == 2 {
		if strings.Trim(arry[0], "`") == r.DB {
			table = strings.Trim(arry[1], "`")
		}
	}
	rule := r.Rules[table]
	if rule == nil {
		return r.DefaultRule
	} else {
		return rule
	}
}

//计算分表规则
func parseRule(db string, cfg *config.ShardConfig) (*Rule, error) {
	r := new(Rule)
	r.DB = db
	r.Table = cfg.Table
	r.Key = cfg.Key
	r.Type = cfg.Type
	r.Nodes = cfg.Nodes //将ruleconfig中的nodes赋值给rule
	r.TableToNode = make(map[int]int, 0)

	switch r.Type {
	case HashRuleType, RangeRuleType:
		var sumTables int
		if len(cfg.Locations) != len(r.Nodes) {
			return nil, errors.ErrLocationsCount
		}
		for i := 0; i < len(cfg.Locations); i++ {
			for j := 0; j < cfg.Locations[i]; j++ {
				r.SubTableIndexs = append(r.SubTableIndexs, j+sumTables)
				r.TableToNode[j+sumTables] = i //建立表下标到节点下标的映射关系,都是从0下标开始
			}
			sumTables += cfg.Locations[i]
		}
	case DateDayRuleType:
		if len(cfg.DateRange) != len(r.Nodes) {
			return nil, errors.ErrDateRangeCount
		}
		for i := 0; i < len(cfg.DateRange); i++ {
			dayNumbers, err := ParseDayRange(cfg.DateRange[i])
			if err != nil {
				return nil, err
			}
			for _, v := range dayNumbers { //建立表时间后缀(天)到节点下标的映射关系,都是从0下标开始
				r.SubTableIndexs = append(r.SubTableIndexs, v)
				r.TableToNode[v] = i
			}
		}
	case DateMonthRuleType:
		if len(cfg.DateRange) != len(r.Nodes) {
			return nil, errors.ErrDateRangeCount
		}
		for i := 0; i < len(cfg.DateRange); i++ {
			monthNumbers, err := ParseMonthRange(cfg.DateRange[i])
			if err != nil {
				return nil, err
			}
			for _, v := range monthNumbers { //建立表时间后缀(月)到节点下标的映射关系,都是从0下标开始
				r.SubTableIndexs = append(r.SubTableIndexs, v)
				r.TableToNode[v] = i
			}
		}
	case DateYearRuleType:
		if len(cfg.DateRange) != len(r.Nodes) {
			return nil, errors.ErrDateRangeCount
		}
		for i := 0; i < len(cfg.DateRange); i++ {
			yearNumbers, err := ParseYearRange(cfg.DateRange[i])
			if err != nil {
				return nil, err
			}
			for _, v := range yearNumbers { //建立表时间后缀(年)到节点下标的映射关系,都是从0下标开始
				r.TableToNode[v] = i
				r.SubTableIndexs = append(r.SubTableIndexs, v)
			}
		}
	}

	if err := parseShard(r, cfg); err != nil {
		return nil, err
	}

	return r, nil
}

/**
 * 获取不同分表规则的计算基数"
 * 时间分表不需要基数，只需要根据分表类型和时间戳进行匹配即可
**/
func parseShard(r *Rule, cfg *config.ShardConfig) error {
	switch r.Type {
	case HashRuleType:
		r.Shard = &HashShard{ShardNum: len(r.TableToNode)}
	case RangeRuleType:
		rs, err := ParseNumSharding(cfg.Locations, cfg.TableRowLimit)
		if err != nil {
			return err
		}

		if len(rs) != len(r.TableToNode) {
			return fmt.Errorf("range space %d not equal tables %d", len(rs), len(r.TableToNode))
		}

		r.Shard = &NumRangeShard{Shards: rs}
	case DateDayRuleType:
		r.Shard = &DateDayShard{}
	case DateMonthRuleType:
		r.Shard = &DateMonthShard{}
	case DateYearRuleType:
		r.Shard = &DateYearShard{}
	default:
		r.Shard = &DefaultShard{}
	}

	return nil
}

func includeNode(nodes []string, node string) bool {
	for _, n := range nodes {
		if n == node {
			return true
		}
	}
	return false
}

//build a router plan
func (r *Router) BuildPlan(statement sqlparser.Statement) (*Plan, error) {
	//因为实现Statement接口的方法都是指针类型，所以type对应类型也是指针类型
	switch stmt := statement.(type) {
	case *sqlparser.Insert:
		return r.buildInsertPlan(stmt)
	case *sqlparser.Replace:
		return r.buildReplacePlan(stmt)
	case *sqlparser.Select:
		return r.buildSelectPlan(stmt)
	case *sqlparser.Update:
		return r.buildUpdatePlan(stmt)
	case *sqlparser.Delete:
		return r.buildDeletePlan(stmt)
	}
	return nil, errors.ErrNoPlan
}

func (r *Router) buildSelectPlan(statement sqlparser.Statement) (*Plan, error) {
	plan := &Plan{}
	var where *sqlparser.Where
	var err error
	var tableName string

	stmt := statement.(*sqlparser.Select)
	switch v := (stmt.From[0]).(type) {
	/* alias语句(简单语句) */
	case *sqlparser.AliasedTableExpr:
		tableName = sqlparser.String(v.Expr)
	/* join语句 */
	case *sqlparser.JoinTableExpr:
		if ate, ok := (v.LeftExpr).(*sqlparser.AliasedTableExpr); ok {
			tableName = sqlparser.String(ate.Expr)
		} else {
			tableName = sqlparser.String(v)
		}
	default:
		tableName = sqlparser.String(v)
	}

	plan.Rule = r.GetRule(tableName) //根据表名获得分表规则
	where = stmt.Where

	if where != nil {
		plan.Criteria = where.Expr //计算路由节点下标和表下标
		err = plan.calRouteIndexs()
		if err != nil {
			golog.Error("Route", "BuildSelectPlan", err.Error(), 0)
			return nil, err
		}
	} else {
		//if shard select without where,send to all nodes and all tables
		plan.RouteTableIndexs = plan.Rule.SubTableIndexs
		plan.RouteNodeIndexs = makeList(0, len(plan.Rule.Nodes))
	}

	if plan.Rule.Type != DefaultRuleType && len(plan.RouteTableIndexs) == 0 {
		golog.Error("Route", "BuildSelectPlan", errors.ErrNoCriteria.Error(), 0)
		return nil, errors.ErrNoCriteria
	}
	//generate sql,如果routeTableindexs为空则表示不分表，不分表则发default node
	err = r.generateSelectSql(plan, stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildInsertPlan(statement sqlparser.Statement) (*Plan, error) {
	plan := &Plan{}
	plan.Rows = make(map[int]sqlparser.Values)
	stmt := statement.(*sqlparser.Insert)
	if _, ok := stmt.Rows.(sqlparser.SelectStatement); ok {
		return nil, errors.ErrSelectInInsert
	}

	if stmt.Columns == nil {
		return nil, errors.ErrIRNoColumns
	}

	//根据sql语句的表，获得对应的分片规则
	plan.Rule = r.GetRule(sqlparser.String(stmt.Table))

	/* TODO OnDup含义 */
	if stmt.OnDup != nil {
		/* set类型的insert语句 */
		err := plan.Rule.checkUpdateExprs(sqlparser.UpdateExprs(stmt.OnDup))
		if err != nil {
			return nil, err
		}
	}

	plan.Criteria = plan.checkValuesType(stmt.Rows.(sqlparser.Values))

	/* 计算分表目标表下标、节点下标 */
	err := plan.calRouteIndexs()
	if err != nil {
		golog.Error("Route", "BuildInsertPlan", err.Error(), 0)
		return nil, err
	}

	/* 重写insert sql语句 */
	err = r.generateInsertSql(plan, stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildUpdatePlan(statement sqlparser.Statement) (*Plan, error) {
	plan := &Plan{}
	var where *sqlparser.Where

	stmt := statement.(*sqlparser.Update)
	plan.Rule = r.GetRule(sqlparser.String(stmt.Table))
	err := plan.Rule.checkUpdateExprs(stmt.Exprs)
	if err != nil {
		return nil, err
	}

	where = stmt.Where
	if where != nil {
		plan.Criteria = where.Expr //路由条件
		err = plan.calRouteIndexs()
		if err != nil {
			golog.Error("Route", "BuildUpdatePlan", err.Error(), 0)
			return nil, err
		}
	} else {
		//if shard update without where,send to all nodes and all tables
		plan.RouteTableIndexs = plan.Rule.SubTableIndexs
		plan.RouteNodeIndexs = makeList(0, len(plan.Rule.Nodes))
	}

	if plan.Rule.Type != DefaultRuleType && len(plan.RouteTableIndexs) == 0 {
		golog.Error("Route", "BuildUpdatePlan", errors.ErrNoCriteria.Error(), 0)
		return nil, errors.ErrNoCriteria
	}
	//generate sql,如果routeTableindexs为空则表示不分表，不分表则发default node
	err = r.generateUpdateSql(plan, stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildDeletePlan(statement sqlparser.Statement) (*Plan, error) {
	plan := &Plan{}
	var where *sqlparser.Where
	var err error

	stmt := statement.(*sqlparser.Delete)
	plan.Rule = r.GetRule(sqlparser.String(stmt.Table))
	where = stmt.Where

	if where != nil {
		plan.Criteria = where.Expr //路由条件
		err = plan.calRouteIndexs()
		if err != nil {
			golog.Error("Route", "BuildUpdatePlan", err.Error(), 0)
			return nil, err
		}
	} else {
		//if shard delete without where,send to all nodes and all tables
		plan.RouteTableIndexs = plan.Rule.SubTableIndexs
		plan.RouteNodeIndexs = makeList(0, len(plan.Rule.Nodes))
	}

	if plan.Rule.Type != DefaultRuleType && len(plan.RouteTableIndexs) == 0 {
		golog.Error("Route", "BuildDeletePlan", errors.ErrNoCriteria.Error(), 0)
		return nil, errors.ErrNoCriteria
	}
	//generate sql,如果routeTableindexs为空则表示不分表，不分表则发default node
	err = r.generateDeleteSql(plan, stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildReplacePlan(statement sqlparser.Statement) (*Plan, error) {
	plan := &Plan{}
	plan.Rows = make(map[int]sqlparser.Values)

	stmt := statement.(*sqlparser.Replace)
	if _, ok := stmt.Rows.(sqlparser.SelectStatement); ok {
		panic(sqlparser.NewParserError("select in replace not allowed"))
	}

	if stmt.Columns == nil {
		return nil, errors.ErrIRNoColumns
	}

	plan.Rule = r.GetRule(sqlparser.String(stmt.Table))

	err := plan.GetIRKeyIndex(stmt.Columns)
	if err != nil {
		return nil, err
	}

	plan.Criteria = plan.checkValuesType(stmt.Rows.(sqlparser.Values))

	err = plan.calRouteIndexs()
	if err != nil {
		golog.Error("Route", "BuildReplacePlan", err.Error(), 0)
		return nil, err
	}

	err = r.generateReplaceSql(plan, stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

//rewrite select sql
func (r *Router) rewriteSelectSql(plan *Plan, node *sqlparser.Select, tableIndex int) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Fprintf("select %v%s",
		node.Comments,
		node.Distinct,
	)

	var prefix string
	//rewrite select expr
	for _, expr := range node.SelectExprs {
		switch v := expr.(type) {
		case *sqlparser.StarExpr:
			//for shardTable.*,need replace table into shardTable_xxxx.
			if string(v.TableName) == plan.Rule.Table {
				fmt.Fprintf(buf, "%s%s_%04d.*",
					prefix,
					plan.Rule.Table,
					tableIndex,
				)
			} else {
				buf.Fprintf("%s%v", prefix, expr)
			}
		case *sqlparser.NonStarExpr:
			//rewrite shardTable.column as a
			//into shardTable_xxxx.column as a
			if colName, ok := v.Expr.(*sqlparser.ColName); ok {
				if string(colName.Qualifier) == plan.Rule.Table {
					fmt.Fprintf(buf, "%s%s_%04d.%s",
						prefix,
						plan.Rule.Table,
						tableIndex,
						string(colName.Name),
					)
				} else {
					buf.Fprintf("%s%v", prefix, colName)
				}
				//if expr has as
				if v.As != nil {
					buf.Fprintf(" as %s", v.As)
				}
			} else {
				buf.Fprintf("%s%v", prefix, expr)
			}
		default:
			buf.Fprintf("%s%v", prefix, expr)
		}
		prefix = ", "
	}
	//insert the group columns in the first of select cloumns
	if len(node.GroupBy) != 0 {
		prefix = ","
		for _, n := range node.GroupBy {
			buf.Fprintf("%s%v", prefix, n)
		}
	}
	buf.Fprintf(" from ")
	switch v := (node.From[0]).(type) {
	case *sqlparser.AliasedTableExpr:
		if len(v.As) != 0 {
			fmt.Fprintf(buf, "%s_%04d as %s",
				sqlparser.String(v.Expr),
				tableIndex,
				string(v.As),
			)
		} else {
			fmt.Fprintf(buf, "%s_%04d",
				sqlparser.String(v.Expr),
				tableIndex,
			)
		}
	case *sqlparser.JoinTableExpr:
		if ate, ok := (v.LeftExpr).(*sqlparser.AliasedTableExpr); ok {
			if len(ate.As) != 0 {
				fmt.Fprintf(buf, "%s_%04d as %s",
					sqlparser.String(ate.Expr),
					tableIndex,
					string(ate.As),
				)
			} else {
				fmt.Fprintf(buf, "%s_%04d",
					sqlparser.String(ate.Expr),
					tableIndex,
				)
			}
		} else {
			fmt.Fprintf(buf, "%s_%04d",
				sqlparser.String(v.LeftExpr),
				tableIndex,
			)
		}
		buf.Fprintf(" %s %v", v.Join, v.RightExpr)
		if v.On != nil {
			buf.Fprintf(" on %v", v.On)
		}
	default:
		fmt.Fprintf(buf, "%s_%04d",
			sqlparser.String(node.From[0]),
			tableIndex,
		)
	}
	//append other tables
	prefix = ", "
	for i := 1; i < len(node.From); i++ {
		buf.Fprintf("%s%v", prefix, node.From[i])
	}

	newLimit, err := node.Limit.RewriteLimit()
	if err != nil {
		//do not change limit
		newLimit = node.Limit
	}
	//rewrite where
	oldright, err := plan.rewriteWhereIn(tableIndex)

	buf.Fprintf("%v%v%v%v%v%s",
		node.Where,
		node.GroupBy,
		node.Having,
		node.OrderBy,
		newLimit,
		node.Lock,
	)
	//restore old right
	if oldright != nil {
		plan.InRightToReplace.Right = oldright
	}
	return buf.String()
}

//重写select sql
func (r *Router) generateSelectSql(plan *Plan, stmt sqlparser.Statement) error {
	sqls := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Select)
	if ok == false {
		return errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexs) == 0 {
		return errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexs) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := r.Nodes[0]
		sqls[nodeName] = []string{buf.String()}
	} else {
		tableCount := len(plan.RouteTableIndexs)
		for i := 0; i < tableCount; i++ {
			tableIndex := plan.RouteTableIndexs[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := r.Nodes[nodeIndex]
			selectSql := r.rewriteSelectSql(plan, node, tableIndex)
			if _, ok := sqls[nodeName]; ok == false {
				sqls[nodeName] = make([]string, 0, tableCount)
			}
			sqls[nodeName] = append(sqls[nodeName], selectSql)
		}
	}
	plan.RewrittenSqls = sqls
	return nil
}

//生成多个节点各自的insert sql语句
func (r *Router) generateInsertSql(plan *Plan, stmt sqlparser.Statement) error {
	sqls := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Insert)
	if ok == false {
		return errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexs) == 0 {
		return errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexs) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := r.Nodes[0]
		sqls[nodeName] = []string{buf.String()}
	} else {
		tableCount := len(plan.RouteTableIndexs)
		for i := 0; i < tableCount; i++ {
			buf := sqlparser.NewTrackedBuffer(nil)
			tableIndex := plan.RouteTableIndexs[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := r.Nodes[nodeIndex]

			buf.Fprintf("insert %v%s into %v", node.Comments, node.Ignore, node.Table)
			//如果是按时间分表，可能超过长度，但都是数字，不影响
			fmt.Fprintf(buf, "_%04d", plan.RouteTableIndexs[i])
			buf.Fprintf("%v %v%v",
				node.Columns,
				plan.Rows[tableIndex],
				node.OnDup)

			if _, ok := sqls[nodeName]; ok == false {
				sqls[nodeName] = make([]string, 0, tableCount)
			}
			sqls[nodeName] = append(sqls[nodeName], buf.String())
		}

	}
	plan.RewrittenSqls = sqls
	return nil
}

func (r *Router) generateUpdateSql(plan *Plan, stmt sqlparser.Statement) error {
	sqls := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Update)
	if ok == false {
		return errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexs) == 0 {
		return errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexs) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := r.Nodes[0]
		sqls[nodeName] = []string{buf.String()}
	} else {
		tableCount := len(plan.RouteTableIndexs)
		for i := 0; i < tableCount; i++ {
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Fprintf("update %v%v",
				node.Comments,
				node.Table,
			)
			fmt.Fprintf(buf, "_%04d", plan.RouteTableIndexs[i])
			buf.Fprintf(" set %v%v%v%v",
				node.Exprs,
				node.Where,
				node.OrderBy,
				node.Limit,
			)
			tableIndex := plan.RouteTableIndexs[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := r.Nodes[nodeIndex]
			if _, ok := sqls[nodeName]; ok == false {
				sqls[nodeName] = make([]string, 0, tableCount)
			}
			sqls[nodeName] = append(sqls[nodeName], buf.String())
		}

	}
	plan.RewrittenSqls = sqls
	return nil
}

func (r *Router) generateDeleteSql(plan *Plan, stmt sqlparser.Statement) error {
	sqls := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Delete)
	if ok == false {
		return errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexs) == 0 {
		return errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexs) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := r.Nodes[0]
		sqls[nodeName] = []string{buf.String()}
	} else {
		tableCount := len(plan.RouteTableIndexs)
		for i := 0; i < tableCount; i++ {
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Fprintf("delete %vfrom %v",
				node.Comments,
				node.Table,
			)
			fmt.Fprintf(buf, "_%04d", plan.RouteTableIndexs[i])
			buf.Fprintf("%v%v%v",
				node.Where,
				node.OrderBy,
				node.Limit,
			)
			tableIndex := plan.RouteTableIndexs[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := r.Nodes[nodeIndex]
			if _, ok := sqls[nodeName]; ok == false {
				sqls[nodeName] = make([]string, 0, tableCount)
			}
			sqls[nodeName] = append(sqls[nodeName], buf.String())
		}

	}
	plan.RewrittenSqls = sqls
	return nil
}

func (r *Router) generateReplaceSql(plan *Plan, stmt sqlparser.Statement) error {
	sqls := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Replace)
	if ok == false {
		return errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexs) == 0 {
		return errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexs) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := r.Nodes[0]
		sqls[nodeName] = []string{buf.String()}
	} else {
		tableCount := len(plan.RouteTableIndexs)
		for i := 0; i < tableCount; i++ {
			tableIndex := plan.RouteTableIndexs[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := r.Nodes[nodeIndex]

			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Fprintf("replace %vinto %v",
				node.Comments,
				node.Table,
			)
			fmt.Fprintf(buf, "_%04d", plan.RouteTableIndexs[i])
			buf.Fprintf("%v %v",
				node.Columns,
				plan.Rows[tableIndex],
			)

			if _, ok := sqls[nodeName]; ok == false {
				sqls[nodeName] = make([]string, 0, tableCount)
			}
			sqls[nodeName] = append(sqls[nodeName], buf.String())
		}

	}
	plan.RewrittenSqls = sqls
	return nil
}
