use std::collections::HashMap;
use std::time::Instant;
use log::{error, info};
use serde::{Deserialize, Serialize};
use crate::types::Response;

/// v1 初版本地限流，当redis失效就用老方法
pub struct V1 {
    pub map: HashMap<i64, u64>,
    pub instant: Instant,
    pub nums: u64,
    pub wait: u64
}

#[derive(Debug, Serialize, Deserialize)]
struct V1Config {
    nums: u64,
    wait: u64
}

impl V1 {
    pub fn new(limit: u64) -> Self
    {
        error!("[Limiter.v1]equip the v1-limiter!");
        V1 {
            map: HashMap::with_capacity(5000),
            instant: Instant::now(),
            nums: limit,
            wait: 0
        }
    }

    pub async fn check(&mut self, bot_id: i64) -> Response
    {
        let mut used = 0;
        let mut wait = 0;
        let elapsed = self.instant.elapsed().as_millis();
        if elapsed > 1000 {
            self.map.clear();
            self.instant = Instant::now();
        } else {
            wait = self.wait;
            if self.nums > 0 {
                let num = self.map.entry(bot_id).or_insert(0);
                if *num <= self.nums {
                    *num += 1;
                }
                used = num.clone();
            }
        }
        let surplus = if self.nums >= used {
            self.nums - used + 1
        } else {
            0
        };
        // 一个短暂延时返回效果
        if surplus==0 {
            tokio::time::sleep(std::time::Duration::from_millis(wait)).await
        }
        Response {
            total: self.nums,
            surplus
        }
    }

    pub async fn get_limit(&self) -> String {
        let data = V1Config {
            nums: self.nums,
            wait: self.wait
        };
        serde_json::to_string(&data).unwrap()
    }

    pub async fn set_limit(&mut self, payload: String) -> bool {
        let config = match serde_json::from_str::<V1Config>(&payload) {
            Ok(c) => c,
            Err(err) => {
                error!("[Limiter:v1]set v1 limiter parse payload error {}", err);
                return false
            }
        };
        let wait = if config.wait > 200 {
            error!("[Limiter:v1]set v1 limiter wait cannot more than 200, set it to 200");
            200
        } else {
            config.wait
        };
        self.nums = config.nums;
        self.wait = wait;
        info!("[Limiter.v1]set v1 ok, {}", serde_json::to_string(&config).unwrap());
        true
    }
}