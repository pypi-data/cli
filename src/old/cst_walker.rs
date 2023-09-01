use crate::stats::Stats;
use libcst_native::*;

#[tracing::instrument(skip(module), level = "debug")]
pub fn walk_cst(module: Module) -> Stats {
    let mut stats = Default::default();
    for statement in module.body {
        handle_statement(statement, &mut stats);
    }
    stats
}

fn handle_statement(statement: Statement, stats: &mut Stats) {
    match statement {
        Statement::Simple(simple) => {
            for line in simple.body {
                handle_small_statement(line, stats);
            }
        }
        Statement::Compound(compound) => {
            handle_compound_statement(compound, stats);
        }
    }
}

#[inline(always)]
fn handle_compound_statement(statement: CompoundStatement, stats: &mut Stats) {
    match statement {
        CompoundStatement::FunctionDef(def) => {
            if def.asynchronous.is_some() {
                stats.has_async = true;
            }
            if def.returns.is_some() {
                stats.has_annotations = true;
            }
            handle_suite(def.body, stats);
        }
        CompoundStatement::If(if_statement) => {
            handle_if(if_statement, stats);
        }
        CompoundStatement::For(For {
            iter,
            body,
            orelse,
            asynchronous,
            ..
        }) => {
            if asynchronous.is_some() {
                stats.has_async = true;
            }
            handle_expression(iter, stats);
            handle_suite(body, stats);
            if let Some(orelse) = orelse {
                handle_suite(orelse.body, stats);
            }
        }
        CompoundStatement::While(While {
            test, body, orelse, ..
        }) => {
            handle_expression(test, stats);
            handle_suite(body, stats);
            if let Some(orelse) = orelse {
                handle_suite(orelse.body, stats);
            }
        }
        CompoundStatement::ClassDef(ClassDef { body, .. }) => {
            handle_suite(body, stats);
        }
        CompoundStatement::Try(Try {
            body,
            handlers,
            orelse,
            finalbody,
            ..
        }) => {
            handle_suite(body, stats);
            for handler in handlers {
                handle_suite(handler.body, stats);
            }
            if let Some(orelse) = orelse {
                handle_suite(orelse.body, stats);
            }
            if let Some(finalbody) = finalbody {
                handle_suite(finalbody.body, stats);
            }
        }
        CompoundStatement::TryStar(TryStar {
            body,
            handlers,
            orelse,
            finalbody,
            ..
        }) => {
            stats.has_try_star = true;
            handle_suite(body, stats);
            for handler in handlers {
                handle_suite(handler.body, stats);
            }
            if let Some(orelse) = orelse {
                handle_suite(orelse.body, stats);
            }
            if let Some(finalbody) = finalbody {
                handle_suite(finalbody.body, stats);
            }
        }
        CompoundStatement::With(With {
            items,
            body,
            asynchronous,
            ..
        }) => {
            if asynchronous.is_some() {
                stats.has_async = true;
            }
            for item in items {
                handle_expression(item.item, stats);
            }
            handle_suite(body, stats);
        }
        CompoundStatement::Match(Match { subject, cases, .. }) => {
            stats.has_match = true;
            handle_expression(subject, stats);
            for case in cases {
                handle_suite(case.body, stats);
            }
        }
    }
}

fn handle_if(if_statement: If, stats: &mut Stats) {
    handle_expression(if_statement.test, stats);
    handle_suite(if_statement.body, stats);
    if let Some(orelse) = if_statement.orelse {
        match *orelse {
            OrElse::Else(Else { body, .. }) => handle_suite(body, stats),
            OrElse::Elif(e) => handle_if(e, stats),
        }
    }
}

#[inline(always)]
fn handle_suite(suite: Suite, stats: &mut Stats) {
    match suite {
        Suite::IndentedBlock(simple) => {
            for statement in simple.body {
                handle_statement(statement, stats);
            }
        }
        Suite::SimpleStatementSuite(suite) => {
            for item in suite.body {
                handle_small_statement(item, stats);
            }
        }
    }
}

fn handle_small_statement(statement: SmallStatement, stats: &mut Stats) {
    match statement {
        SmallStatement::Return(Return {
            value: Some(exp), ..
        }) => handle_expression(exp, stats),
        SmallStatement::Expr(Expr { value, .. }) => handle_expression(value, stats),
        SmallStatement::Assert(Assert { test, .. }) => handle_expression(test, stats),
        SmallStatement::Assign(Assign { value, .. }) => handle_expression(value, stats),
        SmallStatement::AnnAssign(AnnAssign {
            value: Some(value), ..
        }) => {
            stats.has_annotations = true;
            handle_expression(value, stats)
        }
        SmallStatement::AugAssign(AugAssign { value, .. }) => handle_expression(value, stats),
        SmallStatement::Import(Import { names, .. }) => {
            for name in names {
                handle_import_names(name.name, stats);
            }
        }
        SmallStatement::ImportFrom(ImportFrom {
            module: Some(module),
            ..
        }) => {
            handle_import_names(module, stats);
        }
        _ => {}
    }
}

fn handle_import_names(name: NameOrAttribute, stats: &mut Stats) {
    match name {
        NameOrAttribute::N(name) => {
            if name.value == "dataclasses" {
                stats.has_dataclasses = true;
            }
        }
        NameOrAttribute::A(_) => {}
    }
}

#[inline(always)]
fn handle_expression(expression: Expression, stats: &mut Stats) {
    match expression {
        Expression::Comparison(comparison) => {
            handle_expression(*comparison.left, stats);
            for op in comparison.comparisons {
                handle_expression(op.comparator, stats);
            }
        }
        Expression::UnaryOperation(op) => {
            handle_expression(*op.expression, stats);
        }
        Expression::BinaryOperation(op) => {
            if let (Expression::SimpleString(..), BinaryOp::Modulo { .. }) =
                (&*op.left, op.operator)
            {
                stats.has_modulo_formatting = true;
            }
            handle_expression(*op.left, stats);
            handle_expression(*op.right, stats);
        }
        Expression::BooleanOperation(op) => {
            handle_expression(*op.left, stats);
            handle_expression(*op.right, stats);
        }
        Expression::Attribute(op) => {
            handle_expression(*op.value, stats);
        }
        Expression::Tuple(exp) => {
            for element in exp.elements {
                handle_element(element, stats);
            }
        }
        Expression::Call(func) => {
            handle_expression(*func.func, stats);
            for arg in func.args {
                handle_expression(arg.value, stats);
            }
        }
        Expression::GeneratorExp(generator) => {
            stats.has_generator_expression = true;
            handle_expression(*generator.elt, stats);
            handle_comp_for(*generator.for_in, stats);
        }
        Expression::ListComp(comp) => {
            stats.has_list_comp = true;
            handle_expression(*comp.elt, stats);
            handle_comp_for(*comp.for_in, stats);
        }
        Expression::SetComp(comp) => {
            stats.has_set_comp = true;
            handle_expression(*comp.elt, stats);
            handle_comp_for(*comp.for_in, stats);
        }
        Expression::DictComp(comp) => {
            stats.has_dict_comp = true;
            handle_expression(*comp.key, stats);
            handle_expression(*comp.value, stats);
            handle_comp_for(*comp.for_in, stats);
        }
        Expression::List(list) => {
            for element in list.elements {
                handle_element(element, stats);
            }
        }
        Expression::Set(set) => {
            for element in set.elements {
                handle_element(element, stats);
            }
        }
        Expression::Dict(dict) => {
            for element in dict.elements {
                match element {
                    DictElement::Simple { key, value, .. } => {
                        handle_expression(key, stats);
                        handle_expression(value, stats);
                    }
                    DictElement::Starred(StarredDictElement { value, .. }) => {
                        handle_expression(value, stats);
                    }
                }
            }
        }
        Expression::Subscript(subscript) => {
            handle_expression(*subscript.value, stats);
        }
        Expression::StarredElement(starred) => {
            handle_expression(*starred.value, stats);
        }
        Expression::IfExp(if_expr) => {
            handle_expression(*if_expr.test, stats);
            handle_expression(*if_expr.body, stats);
            handle_expression(*if_expr.orelse, stats);
        }
        Expression::Lambda(lambda) => {
            handle_expression(*lambda.body, stats);
        }
        Expression::Yield(yield_expr) => {
            if let Some(value) = yield_expr.value {
                match *value {
                    YieldValue::Expression(exp) => {
                        handle_expression(*exp, stats);
                    }
                    YieldValue::From(from_expr) => {
                        handle_expression(from_expr.item, stats);
                    }
                }
            }
        }
        Expression::Await(await_expr) => {
            stats.has_async = true;
            handle_expression(*await_expr.expression, stats);
        }
        Expression::FormattedString(_) => {
            stats.has_fstring = true;
        }
        Expression::NamedExpr(named) => {
            stats.has_walrus = true;
            handle_expression(*named.target, stats);
            handle_expression(*named.value, stats);
        }
        _ => {}
    }
}

#[inline(always)]
fn handle_element(element: Element, stats: &mut Stats) {
    match element {
        Element::Simple { value, .. } => {
            handle_expression(value, stats);
        }
        Element::Starred(starred) => {
            handle_expression(*starred.value, stats);
        }
    }
}

#[inline(always)]
fn handle_assignable_target_expression(expr: AssignTargetExpression, stats: &mut Stats) {
    match expr {
        AssignTargetExpression::Attribute(attr) => {
            handle_expression(*attr.value, stats);
        }
        AssignTargetExpression::StarredElement(starred) => {
            handle_expression(*starred.value, stats);
        }
        AssignTargetExpression::List(list) => {
            for element in list.elements {
                handle_element(element, stats);
            }
        }
        AssignTargetExpression::Tuple(tuple) => {
            for element in tuple.elements {
                handle_element(element, stats);
            }
        }
        AssignTargetExpression::Subscript(sub) => {
            handle_expression(*sub.value, stats);
        }

        _ => {}
    }
}

fn handle_comp_for(for_in: CompFor, stats: &mut Stats) {
    if for_in.asynchronous.is_some() {
        stats.has_async_comp = true;
    }
    handle_assignable_target_expression(for_in.target, stats);
    handle_expression(for_in.iter, stats);
    for if_expr in for_in.ifs {
        handle_expression(if_expr.test, stats);
    }
    if let Some(inner_for) = for_in.inner_for_in {
        handle_comp_for(*inner_for, stats);
    }
}
