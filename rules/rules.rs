use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub position: i32,
    pub expression: String,
    pub actions: Vec<Action>,

    pub project_id: Option<Uuid>,
    pub ruleset_id: Option<Uuid>,
}

pub type CompiledExpression = bel::Program;
pub type Context<'a> = bel::Context<'a>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "limit", rename_all = "snake_case")]
pub struct RateLimit {
    pub max: u16,
    pub window: u16,
    pub capacity: RateLimitBucketSize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "limit", rename_all = "snake_case")]
/// Number is power (exponent) of 2 -- it defines number of unique IPs that can be tracked
pub enum RateLimitBucketSize {
    Bucket10 = 2isize.pow(10),
    Bucket14 = 2isize.pow(14),
    Bucket16 = 2isize.pow(16),
    Bucket17 = 2isize.pow(17),
    Bucket19 = 2isize.pow(19),
    Bucket20 = 2isize.pow(20),
    Bucket23 = 2isize.pow(23),
    Bucket24 = 2isize.pow(24),
}

// pub struct CompiledRule {
//     pub id: Uuid,
//     pub updated_at: DateTime<Utc>,
// }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    Block {},
    Captcha {},
    Limit {},
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Unspecified(String),
    #[error("Expression is not valid: {0}")]
    ExpressionIsNotValid(String),
}

pub fn compile_expression(expression: &str) -> Result<CompiledExpression, Error> {
    let program = match std::panic::catch_unwind(|| bel::Program::compile(expression)) {
        Ok(Ok(program)) => program,
        Ok(Err(err)) => return Err(Error::ExpressionIsNotValid(err.to_string())),
        Err(_) => return Err(Error::ExpressionIsNotValid("invalid input".to_string())),
    };

    return Ok(program);
}

pub fn validate_expression(expression: &str) -> Result<(), Error> {
    if expression.is_empty() {
        return Err(Error::ExpressionIsNotValid("expression is empty".to_string()));
    }

    let program = match std::panic::catch_unwind(|| bel::Program::compile(expression)) {
        Ok(Ok(program)) => program,
        Ok(Err(err)) => return Err(Error::ExpressionIsNotValid(err.to_string())),
        Err(_) => return Err(Error::ExpressionIsNotValid("invalid input".to_string())),
    };
    let references = program.references();

    // validate functions
    let functions = references.functions();
    if functions.contains(&"@in") {
        return Err(Error::ExpressionIsNotValid("unknown operator: in".to_string()));
    }

    // validate variables
    // TODO

    return Ok(());
}
