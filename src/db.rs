use std::collections::HashMap;
use scylla::{Session, SessionBuilder, FromRow, IntoTypedRows, query::*};
use scylla::transport::load_balancing::RoundRobinPolicy;
use scylla::frame::response::result::CqlValue;
use scylla_cql::{Consistency, errors::NewSessionError};
use std::sync::Arc;
use log::error;
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};

pub struct DBRepo {
    session: Session,
}

impl DBRepo {
    pub async fn new(hosts: &Vec<String>, username: &str, password: &str) -> Result<DBRepo, NewSessionError> {
        match SessionBuilder::new()
            .known_nodes(hosts)
            .user(username, password)
            .load_balancing(Arc::new(RoundRobinPolicy::new()))
            .default_consistency(Consistency::LocalOne)
            .build()
            .await {
            Ok(session) => Ok(DBRepo { session }),
            Err(_e) => Err(_e)
        }
    }

    /// 读取持久化配置信息
    pub async fn read(&self) -> Result<String, Error>
    {
        let smt = r#"SELECT version, config, updated_at FROM xbot.limits"#;
        if let Some(rows) = self.session.query(smt.clone(), &[]).await.map_err(|err|{
            error!("[limiter:db]failed to excute smt={} with err={:?}", smt, err);
            Error::new(ErrorKind::Interrupted, err)
        }).map_err(|err| Error::new(ErrorKind::Interrupted, err))?.rows {
            for row in rows.into_typed::<Limit>() {
                let limit = row.map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
                return Ok(limit.config);
            }
        }
        Ok("".to_string())
    }

    /// 写入配置信息
    pub async fn write(&self, config: String, version: String) -> Result<(), Error>
    {
        let smt = r#"UPDATE xbot.limits SET config=:config,updated_at=:updated_at where version=:version"#;
        let mut vals: HashMap<&str, CqlValue> = HashMap::new();
        vals.insert("config", CqlValue::Text(config));
        let now = chrono::Local::now().timestamp_millis();
        vals.insert("updated_at", CqlValue::BigInt(now));
        vals.insert("version", CqlValue::Text(version));
        let mut query = Query::new(smt);
        query.set_consistency(Consistency::LocalQuorum);
        match self.session.query(query.clone(), &vals).await {
            Ok(r) => {
                match r.result_not_rows() {
                    Ok(()) => Ok(()),
                    Err(err) => {
                        error!("[limiter:db]write result error {:?}", err);
                        Err(Error::new(ErrorKind::Other, err))
                    }
                }
            }
            Err(err) => {
                error!("[limiter:db]write query error {:?}", err);
                Err(Error::new(ErrorKind::Other, err))
            }
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, FromRow)]
pub struct Limit {
    pub version: String,
    pub config: String,
    pub updated_at: i64,
}