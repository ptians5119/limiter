use std::collections::HashMap;
use crate::limiter::Statistics;

/// 限流统计
pub fn statistic(map: &mut Statistics, bot: i64, key: String, allow: bool)
{
    let key = (bot, key);
    if let Some(value) = map.get_mut(&key) {
        match allow {
            true => (*value).0 += 1,
            false => (*value).1 += 1
        }
    } else {
        match allow {
            true => { let _ = map.insert(key, (1, 0)); },
            false => { let _ = map.insert(key, (0, 1)); }
        }
    }
}

/// 生成当日报告, 几个节点就有几份map
pub fn report(maps: Vec<Statistics>) -> String
{
    let date = chrono::Local::today().format("%Y-%m-%d").to_string();
    let mut map0 = maps[0].clone();
    let len = maps.len();
    for inx in 1..len {
        for (key, val) in &maps[inx] {
            if let Some(value) = map0.get_mut(key) {
                let tmp = val.clone();
                (*value).0 += tmp.0;
                (*value).1 += tmp.1;
            }
        }
    }
    let mut report = String::new();
    // 继续提炼一下数据，组织成 bot: content的形式
    let mut new_map = HashMap::new();
    for item in map0 {
        let key = item.0.clone();
        let val = item.1.clone();
        let content = format!("+{}  allow:{}  deny:{}\n", &key.1, val.0, val.1);
        let map_value = new_map.entry(key.0).or_insert("".to_string());
        *map_value = map_value.to_owned() + content.as_str();
    }
    report = report + ">>>>>> Limiter.report: " + date.as_str() + "\n";
    for (bot, content) in new_map {
        report = report + "bot:" + (bot.to_string()).as_str() + "\n";
        report = report + content.as_str();
    }
    report = report + "<<<<<< over.";
    report
}