// 策略生成器

use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use log::error;
use serde_json::Value;
use crate::types::Level;
use crate::types::Ratio;
use crate::limiter::Strategies;

/// 生成策略信息
pub fn generate(lev: &Level, rat: &Ratio) -> Result<Strategies, Error>
{
    let mut strategies = HashMap::new();
    let ratios = rat.map();
    for (total, bots) in lev.map() {
        for bot in bots {
            let ratio = if ratios.contains_key(&bot) {
                ratios.get(&bot).unwrap().to_string()
            } else {
                "".to_string()
            };
            let strategy = generate_strategy_by_bot(total, ratio)?;
            strategies.insert(bot, strategy);
        }
    }
    Ok(strategies)
}

/// 根据bot_id、total等信息生成每个bot的策略
fn generate_strategy_by_bot(total: u64, ratio: String) -> Result<String, Error>
{
    if ratio.eq("") {
        return Ok(Strategy::Default(total).to_str())
    }
    let val = match serde_json::from_str::<Value>(&ratio) {
        Ok(v) => v,
        Err(err) => {
            error!("[Limiter:strategy.rs] parse ratio error {}", err);
            return Err(Error::new(ErrorKind::InvalidData, "解析ratio失败"));
        }
    };
    let mut sum = 0;
    if let Some(object) = val.as_object() {
        let mut strategy = HashMap::new();
        for (api, num) in object {
            // 两种情形，整数型 + 小数型
            let num = if num.is_f64() {
                (num.as_f64().unwrap() * (total as f64)) as u64
            } else {
                num.as_u64().unwrap()
            };
            sum += num;
            if sum > total {
                error!("[Limiter:strategy.rs] sum is bigger than total");
                return Err(Error::new(ErrorKind::InvalidData, "ratio内容不正确，sum过大"));
            }
            strategy.insert(api.clone(), num);
        }
        strategy.insert("other".to_string(), total - sum);

        Ok(Strategy::Map(strategy).to_str())
    } else {
        Ok(Strategy::Default(total).to_str())
    }
}

/// 获得具体限流次数
pub fn limit(map: &Strategies, bot: i64, key: String) -> Result<(u64, String), Error>
{
    if let Some(val) = map.get(&bot) {
        let data = match serde_json::from_str::<Value>(val) {
            Ok(v) => v,
            Err(err) => {
                error!("[Limiter:strategy]parse strategy error:{}", err);
                return Err(Error::new(ErrorKind::InvalidData, "解析策略数据失败"))
            }
        };
        if let Some(object) = data.as_object() {
            if object.contains_key(key.as_str()) {
                let num = object.get(key.as_str()).unwrap();
                Ok((num.as_u64().unwrap(), key.to_string()))
            } else {
                let num = object.get("other").unwrap();
                Ok((num.as_u64().unwrap(), "other".to_string()))
            }
        } else {
            Ok((0, "other".to_string()))
        }
    } else {
        Ok((0, "other".to_string()))
    }
}

/// 生成string内容
enum Strategy {
    // 默认值
    Default(u64),
    // 设定值
    Map(HashMap<String, u64>)
}

impl Strategy {
    fn to_str(&self) -> String {
        match self {
            Strategy::Default(u) => {
                let mut val = Value::Null;
                val["other"] = Value::from(*u);
                val.to_string()
            },
            Strategy::Map(map) => {
                let mut val = Value::Null;
                for item in map {
                    val[item.0.as_str()] = Value::from(*item.1);
                }
                val.to_string()
            }
        }
    }
}