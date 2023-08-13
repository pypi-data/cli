use libcst_native::*;

#[derive(Debug, Default)]
pub struct Stats {
    pub has_async: bool,
    pub has_fstring: bool,
    pub has_annotations: bool,
    pub has_try_star: bool,
    pub has_match: bool,
    pub has_matrix_multiply: bool,
}

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
        _ => {}
    }
}

fn handle_expression(expression: Expression, stats: &mut Stats) {
    match expression {
        // Expression::Name(_) => {}
        // Expression::Ellipsis(_) => {}
        // Expression::Integer(_) => {}
        // Expression::Float(_) => {}
        // Expression::Imaginary(_) => {}
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
                todo!("{element:?}");
                // handle_expression(element, stats);
            }
        }
        Expression::Call(func) => {
            handle_expression(*func.func, stats);
            for arg in func.args {
                handle_expression(arg.value, stats);
            }
        }
        Expression::GeneratorExp(generator) => {
            handle_expression(*generator.elt, stats);
            todo!("generator for_in");
        }
        Expression::ListComp(comp) => {
            handle_expression(*comp.elt, stats);
            todo!("list for_in");
        }
        Expression::SetComp(comp) => {
            handle_expression(*comp.elt, stats);
            todo!("set for_in");
        }
        Expression::DictComp(comp) => {
            handle_expression(*comp.key, stats);
            handle_expression(*comp.value, stats);
            todo!("dict for_in");
        }
        Expression::List(list) => {
            for element in list.elements {
                todo!("{element:?}");
                // handle_expression(element, stats);
            }
        }
        Expression::Set(set) => {
            for element in set.elements {
                todo!("{element:?}");
                // handle_expression(element, stats);
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
            stats.has_async = true;
        }
        Expression::NamedExpr(named) => {
            handle_expression(*named.target, stats);
            handle_expression(*named.value, stats);
        }
        _ => {}
    }
}
