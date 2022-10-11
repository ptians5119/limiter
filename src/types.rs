use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::io::Error;
use crate::limiter::Strategies;
use crate::strategy;

/// 存储的配置信息
#[derive(Deserialize, Serialize)]
pub struct Config {
    pub ratio: Ratio,
    pub level: Level
}

impl Config {
    // /// 把能正常获取的config下落到db里
    // pub fn store_to_db(&self) -> Result<(), Error> {
    //     Err(Error::new(ErrorKind::Other, "写db失败"))
    // }
    pub fn default() -> Self
    {
        Config {
            ratio: Ratio::new(HashMap::new()),
            level: Level::new(HashMap::new())
        }
    }

    /// 得到strategies
    pub fn get_strategies(&self) -> Result<Strategies, Error>
    {
        strategy::generate(&self.level, &self.ratio)
    }
}

/// 接口配比
#[derive(Deserialize, Serialize)]
pub struct Ratio {
    map: HashMap<i64, String>
}

impl Ratio {
    pub fn new(map: HashMap<i64, String>) -> Self {
        Ratio {
            map
        }
    }

    pub fn map(&self) -> HashMap<i64, String>
    {
        self.map.clone()
    }
}

/// 级别设置
#[derive(Deserialize, Serialize)]
pub struct Level {
    map: HashMap<u64, Vec<i64>>,
}

impl Level {
    pub fn new(map: HashMap<u64, Vec<i64>>) -> Self {
        Level {
            map
        }
    }

    pub fn map(&self) -> HashMap<u64, Vec<i64>>
    {
        self.map.clone()
    }
}

/// 返回信息
#[derive(Serialize)]
pub struct Response {
    pub total: u64,
    pub surplus: u64
}

impl Default for Response {
    fn default() -> Self {
        Response {
            total: 0,
            surplus: 0
        }
    }
}