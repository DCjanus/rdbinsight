use std::task::Poll;

use rdbinsight::{
    helper::AnyResult,
    parser::{Item, Parser},
};

pub fn collect_items(bytes: &[u8]) -> AnyResult<Vec<Item>> {
    let mut parser = Parser::default();
    parser.feed(bytes)?;

    let mut items = Vec::new();
    loop {
        match parser.parse_next()? {
            Poll::Ready(Some(item)) => items.push(item),
            Poll::Ready(None) => break,
            Poll::Pending => continue,
        }
    }

    Ok(items)
}

pub fn filter_records(items: Vec<Item>) -> Vec<Item> {
    items
        .into_iter()
        .filter(|item| !matches!(item, Item::Aux { .. }))
        .filter(|item| !matches!(item, Item::SelectDB { .. }))
        .filter(|item| !matches!(item, Item::ResizeDB { .. }))
        .collect()
}
