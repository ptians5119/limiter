use log::error;
use redis::{Client, RedisResult, ConnectionInfo, Connection};
use std::str::FromStr;

/// limiter的redis用单节点来处理
pub struct RedisRepo {
    pub redis: Client
}

impl RedisRepo {
    pub fn open(url: &str, pwd: &str) -> RedisResult<RedisRepo>
    {
        let mut conn_info = ConnectionInfo::from_str(url)?;
        conn_info.redis.password = Some(pwd.to_string());
        match Client::open(conn_info) {
            Ok(cli) => Ok(RedisRepo { redis: cli }),
            Err(_e) => {
                error!("[limiter:redis]open {} error", url);
                Err(_e)
            }
        }
    }

    pub fn get_connection(&self) -> RedisResult<Connection> {
        self.redis.get_connection()
    }
}