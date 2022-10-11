mod db;
mod v1;
mod redis;
mod types;
mod strategy;
mod statistician;

pub mod limiter {
    use std::collections::HashMap;
    use std::io::{Error, ErrorKind};
    use log::{error, info};
    use redis::Script;
    use crate::{types::*, strategy, redis::RedisRepo, db::DBRepo};
    use crate::statistician::statistic;
    use crate::v1::V1;

    pub type Strategies = HashMap<i64, String>;
    pub type Statistics = HashMap<(i64, String), (u64, u64)>;
    const REDIS_KEY: &str = "limiter:bot_api";

    /// 限流器
    pub struct Limiter {
        default: u64, //默认全域限流次数
        strategies: Strategies, //最后生成的策略组信息
        stop: bool, //是否停止限流
        statistic: Statistics,  //统计信息 (bot_id: api):(pass: limit)
        redis: Option<RedisRepo>,  //redis实例
        db: Option<DBRepo>,  //db实例
        v1: Option<V1>, //第一版限流器
    }

    impl Limiter {
        /// 1.创建一个新的Limiter，给Config用
        pub fn new(default: u64) -> Self {
            Limiter {
                default,
                strategies: HashMap::new(),
                stop: true,
                statistic: HashMap::new(),
                redis: None,
                db: None,
                v1: None,
            }
        }

        /// 获取限流次数
        pub fn get_limit(&self, bot: i64, api: &str) -> Result<(u64, String), Error>
        {
            let u = strategy::limit(&self.strategies, bot, api.to_string())?;
            if u.0==0 {
                Ok((self.default, u.1))
            } else {
                Ok(u)
            }
        }

        /// 执行限流检测脚本 key就是api
        pub async fn check(&mut self, bot: i64, api: &str, key: &str, limit: u64) -> Result<Response, Error>
        {
            if self.stop {
                // 返回默认值表示未开启限流
                return Ok(Response::default())
            }

            // 若v1被启动，则不执行后续动作
            if let Some(mut v1) = self.v1.take() {
                println!("v1");
                let res = v1.check(bot).await;
                self.v1 = Some(v1);
                statistic(&mut self.statistic, bot, api.to_string(), res.surplus!=0);
                return Ok(res)
            }

            let conn = if let Some(my_redis) = self.redis.take() {
                let conn = my_redis
                    .get_connection()
                    .map_err(|err| Error::new(ErrorKind::NotFound, err))?;
                self.redis = Some(my_redis);
                Some(conn)
            } else {
                None
            };

            if conn.is_none() {
                // 走初版限流器
                println!("v1 start");
                info!("[Limiter.lib]setup v1 version");
                if self.v1.is_none() {
                    self.equip_v1();
                }
                if let Some(mut v1) = self.v1.take() {
                    let res = v1.check(bot).await;
                    self.v1 = Some(v1);
                    statistic(&mut self.statistic, bot, api.to_string(), res.surplus!=0);
                    Ok(res)
                } else {
                    Ok(Response::default())
                }
            } else {
                // 走新版限流器
                let mut conn = conn.unwrap();
                let filed = format!("{}:{}", bot, key);
                let now = chrono::Local::now().timestamp_millis();
                // println!("now {}", now);
                // 1.key 2.field 3.limit 4.instant
                let lua = r#"
                    local r = redis.call('HEXISTS', ARGV[1], ARGV[2])
                    if(r==0)
                    then
                        local new_json = {
                            ["instant"] = ARGV[4],
                            ["current"] = 1
                        }
                        local new_str = cjson.encode(new_json)
                        redis.call('HSET', ARGV[1], ARGV[2], new_str)
                        return 0
                    else
                        local j_str = redis.call('HGET', ARGV[1], ARGV[2])
                        local json = cjson.decode(j_str)

                        local tmp = ARGV[4] - json.instant
                        if(tmp>1000)
                        then
                            json.instant = ARGV[4]
                            json.current = 1
                            local new_str = cjson.encode(json)
                            redis.call('HSET', ARGV[1], ARGV[2], new_str)
                            return 0
                        else
                            local tmp = ARGV[3] - json.current
                            if(tmp>0)
                            then
                                json.current = json.current + 1
                                local new_str = cjson.encode(json)
                                redis.call('HSET', ARGV[1], ARGV[2], new_str)
                                return json.current - 1
                            else
                                return json.current
                            end
                        end
                    end
                "#;
                let script = Script::new(lua);
                let result = script
                    .arg(REDIS_KEY)
                    .arg(filed.as_str())
                    .arg(limit)
                    .arg(now)
                    .invoke::<u64>(&mut conn);
                let surplus = match result {
                    Ok(u) => limit-u,
                    Err(err) => {
                        error!("[Limiter:lib]run lua error:{}", err);
                        return Err(Error::new(ErrorKind::Interrupted, "限流运行错误"))
                    }
                };
                let res = Response {
                    total: limit,
                    surplus
                };
                // 统计操作
                statistic(&mut self.statistic, bot, api.to_string(), res.surplus!=0);
                Ok(res)
            }
        }

        /// 执行清空缓存脚本
        pub async fn clear(&mut self) -> Result<(), Error>
        {
            if !self.stop {
                return Err(Error::new(ErrorKind::Other, "未关闭限流"))
            }
            if let Some(my_redis) = self.redis.take() {
                let mut conn = my_redis
                    .get_connection()
                    .map_err(|err| Error::new(ErrorKind::NotFound, err))?;
                self.redis = Some(my_redis);
                let lua = r#"return redis.call('del', ARGV[1])"#;
                let script = Script::new(lua);
                let result = script
                    .arg(REDIS_KEY)
                    .invoke::<usize>(&mut conn);
                match result {
                    Ok(1) => Ok(()),
                    _ => Err(Error::new(ErrorKind::Other, "清空失败"))
                }
            } else {
                Err(Error::new(ErrorKind::Other, "清空失败"))
            }
        }

        /// 2.设置repo
        pub async fn set_repo(
            mut self,
            redis_url: &str,
            redis_password: &str,
            db_hosts: &Vec<String>,
            db_username: &str,
            db_password: &str) -> Self {
            match RedisRepo::open(redis_url, redis_password) {
                Ok(repo) => self.redis = Some(repo),
                Err(err) => error!("[limiter:lib]set redis repo error {:?}", err)
            }
            match DBRepo::new(db_hosts, db_username, db_password).await {
                Ok(repo) => self.db = Some(repo),
                Err(err) => error!("[limiter:lib]set db repo error {:?}", err)
            }
            self
        }

        /// 3.开启服务
        pub async fn run(mut self) -> Result<Self, Error>
        {
            let config = self.get_config_by_db().await?;
            let config = parse_config(config)?;
            let strategies = config.get_strategies()?;
            self.set_strategies(strategies);
            self.start();
            Ok(self)
        }

        /// 4.重设服务
        pub async fn reset(&mut self, config: String) -> Result<(), Error>
        {
            let _config = parse_config(config.clone())?;
            let strategies = _config.get_strategies()?;
            self.set_strategies(strategies);
            self.start();
            if let Some(repo) = self.db.take() {
                // version目前固定值为0.1
                let _ = repo.write(config, "0.1".to_string()).await?;
                self.db = Some(repo);
            }
            Ok(())
        }

        /// 设置strategies
        fn set_strategies(&mut self, map: Strategies)
        {
            self.strategies = map;
        }

        /// 停止限流
        pub fn stop(&mut self) {
            self.stop = true
        }

        /// 开启限流
        fn start(&mut self) {
            self.stop = false
        }

        /// 读取db配置
        pub async fn get_config_by_db(&mut self) -> Result<String, Error>
        {
            let mut config = String::new();
            if let Some(repo) = self.db.take() {
                match repo.read().await {
                    Ok(c) => config = c,
                    Err(err) => error!("[Limiter:lib]read db error: {}", err)
                }
                self.db = Some(repo);
            }
            Ok(config)
        }

        /// 限流统计读取并清空
        pub fn flush(&mut self) -> Statistics
        {
            let tmp = self.statistic.clone();
            self.statistic = HashMap::new();
            tmp
        }

        /// 启用第一版限流器
        fn equip_v1(&mut self)
        {
            self.v1 = Some(V1::new(self.default / 2))
        }
    }

    /// 从配置文本字符串获得config信息，该方法还可以校验config是否正确
    fn parse_config(str: String) -> Result<Config, Error>
    {
        if str.eq("") {
            return Ok(Config::default())
        }
        let config = serde_json::from_str::<Config>(str.as_str())?;
        // let _ = config.store_to_db()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::rc::Rc;
    use std::cell::RefCell;
    use crate::limiter::Limiter;
    use crate::statistician::report;
    use crate::types::{Config, Ratio, Level};
    use tokio::runtime::Runtime;

    async fn get_limiter() -> Limiter {
        Limiter::new(5)
            .set_repo(
                "redis://106.52.192.252:6379",
                "prepared9",
                &vec!["10.2.18.13:9042".to_string()],
                "cassandra",
                "Brysj@1gsycl"
            ).await.run().await.unwrap()
    }

    #[test]
    /// 正常启动
    fn work() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let limiter = get_limiter().await;
            let bot = 1_i64;
            let limit = limiter.get_limit(bot, "whatever").unwrap();
            let limiter = Rc::new(RefCell::new(limiter));
            for _i in 0..30 {
                match (*limiter.clone()).borrow_mut()
                    .check(bot, "whatever1", limit.1.as_str(), limit.0)
                    .await {
                    Ok(r) => println!("{}", serde_json::to_string(&r).unwrap()),
                    Err(err) => println!("{}", err)
                }
                match (*limiter.clone()).borrow_mut()
                    .check(bot, "whatever2", limit.1.as_str(), limit.0)
                    .await {
                    Ok(r) => println!("{}", serde_json::to_string(&r).unwrap()),
                    Err(err) => println!("{}", err)
                }
            }
            let rep = (*limiter.clone()).borrow_mut().flush();
            println!("{}", report(vec![rep]));
        });
    }

    #[test]
    /// 重设
    fn reset() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut ratio = HashMap::new();
            ratio.insert(1_i64, "".to_string());
            let mut level = HashMap::new();
            level.insert(8_u64, vec![1_i64]);
            let config = Config {
                ratio: Ratio::new(ratio),
                level: Level::new(level)
            };
            let config = serde_json::to_string(&config).unwrap();

            let mut limiter = get_limiter().await;
            limiter.stop();
            let _ = limiter.clear().await;
            match limiter.reset(config).await {
                Ok(()) => (),
                Err(err) => println!("{}", err)
            }
        });
    }

    #[test]
    /// 启用v1
    fn v1_test()
    {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let limiter = Limiter::new(100)
                .set_repo(
                    "redis://106.52.192.252:6379",
                    "wonderful",
                    &vec!["10.2.18.13:9042".to_string()],
                    "cassandra",
                    "Brysj@1gsycl"
                ).await.run().await.unwrap();
            let bot = 1_i64;
            let limit = limiter.get_limit(bot, "whatever").unwrap();
            let limiter = Rc::new(RefCell::new(limiter));
            for _i in 0..70 {
                match (*limiter.clone()).borrow_mut()
                    .check(bot, "whatever", limit.1.as_str(), limit.0).await {
                    Ok(r) => println!("{}", serde_json::to_string(&r).unwrap()),
                    Err(err) => println!("{}", err)
                }
            }
            let rep = (*limiter.clone()).borrow_mut().flush();
            println!("{}", report(vec![rep]));
        })
    }
}