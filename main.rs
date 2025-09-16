use anyhow::Result;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};
use uuid::Uuid;

// --- 설정 (Configuration) ---
const BYBIT_API_KEY: &str = "";
const BYBIT_API_SECRET: &str = "";
const BINANCE_API_KEY: &str = "";
const BINANCE_API_SECRET: &str = "";

// --- 트레이딩 파라미터 (Trading Parameters) ---
const TARGET_SYMBOL: &str = "HIFIUSDT";
const SIZE: &str = "9000";
const MAX_ENTRY_COUNT: u32 = 1;
const ENTRY_GAP: f64 = 985.0;
const TARGET_GAP: f64 = ENTRY_GAP + 20.0;

// --- 데이터 구조체 (Data Structures) ---

#[derive(Debug, Clone, Copy, PartialEq)]
enum PositionStatus {
    None,
    BybitLong,
    BinanceLong,
}

#[derive(Debug, Default, Clone)]
struct PriceData {
    ask_price: f64,
    bid_price: f64,
}

#[derive(Debug, Clone, Copy)]
struct PositionState {
    status: PositionStatus,
    entry_count: u32,
}

// 애플리케이션의 공유 상태
struct AppState {
    bybit_prices: RwLock<HashMap<String, PriceData>>,
    binance_prices: RwLock<HashMap<String, PriceData>>,
    position: RwLock<PositionState>,
}

// --- HFT 아키텍처를 위한 신규 데이터 구조체 ---

#[derive(Debug, Clone, Copy)]
enum Exchange {
    Bybit,
    Binance,
}

#[derive(Debug, Clone)]
enum ReportType {
    OrderAccepted,
    OrderRejected(String),
}

// 주문의 의도를 명시하기 위한 enum
#[derive(Debug, Clone, Copy)]
enum OrderIntent {
    Enter,
    Add,
    Close,
}

// 주문 요청 메시지 (상태 보정을 위한 컨텍스트 추가)
#[derive(Debug, Clone)]
struct OrderRequest {
    symbol: String,
    side: String,
    qty: String,
    reduce_only: bool,
    intent: OrderIntent,
    position_status_before_action: PositionStatus, // 이 주문이 발생하기 직전의 포지션 상태
}

// 거래소로부터의 실제 실행 보고
#[derive(Debug, Clone)]
struct ExecutionReport {
    exchange: Exchange,
    report_type: ReportType,
    original_request: OrderRequest,
}


// --- 초기 데이터 로딩 (기존과 동일) ---
async fn initialize_data(state: &AppState) -> Result<()> {
    info!("초기 데이터 로딩 시작...");
    let client = reqwest::Client::new();
    let bybit_url = "https://api.bybit.com/v5/market/tickers?category=linear";
    let bybit_resp: serde_json::Value = client.get(bybit_url).send().await?.json().await?;
    if let Some(list) = bybit_resp["result"]["list"].as_array() {
        let mut prices = state.bybit_prices.write().await;
        for item in list {
            if let Some(symbol) = item["symbol"].as_str() {
                if symbol.ends_with("USDT") {
                    prices.insert(symbol.to_string(), PriceData::default());
                }
            }
        }
    }
    let binance_url = "https://fapi.binance.com/fapi/v1/ticker/bookTicker";
    let binance_resp: Vec<serde_json::Value> = client.get(binance_url).send().await?.json().await?;
    let mut prices = state.binance_prices.write().await;
    for item in binance_resp {
        if let (Some(symbol), Some(ask), Some(bid)) = (
            item["symbol"].as_str(),
            item["askPrice"].as_str().and_then(|p| p.parse().ok()),
            item["bidPrice"].as_str().and_then(|p| p.parse().ok()),
        ) {
            prices.insert(symbol.to_string(), PriceData { ask_price: ask, bid_price: bid });
        }
    }
    info!("✅ 초기 데이터 로딩 완료.");
    Ok(())
}

// --- 웹소켓 핸들러 (Public Stream은 기존과 동일) ---
async fn bybit_public_stream_task(app_state: Arc<AppState>) {
    let url = format!("wss://stream.bybit.com/v5/public/linear?symbol={}", TARGET_SYMBOL);
    loop {
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                info!("✅ Bybit Public WebSocket 연결 성공.");
                let (mut write, mut read) = ws_stream.split();
                let subscribe_msg = json!({"op": "subscribe", "args": [format!("orderbook.1.{}", TARGET_SYMBOL)]});
                if write.send(Message::Text(subscribe_msg.to_string())).await.is_err() { continue; }
                while let Some(Ok(msg)) = read.next().await {
                    if let Message::Text(text) = msg {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let (Some(symbol), Some(asks), Some(bids)) = (data["data"]["s"].as_str(), data["data"]["a"].as_array(), data["data"]["b"].as_array()) {
                                let mut prices = app_state.bybit_prices.write().await;
                                let entry = prices.entry(symbol.to_string()).or_default();
                                if let Some(ask_val) = asks.get(0).and_then(|v| v[0].as_str().and_then(|p| p.parse().ok())) { entry.ask_price = ask_val; }
                                if let Some(bid_val) = bids.get(0).and_then(|v| v[0].as_str().and_then(|p| p.parse().ok())) { entry.bid_price = bid_val; }
                            }
                        }
                    }
                }
            }
            Err(e) => error!("❌ Bybit Public WebSocket 연결 오류: {}. 5초 후 재연결...", e),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn binance_public_stream_task(app_state: Arc<AppState>) {
    let url = format!("wss://fstream.binance.com/ws/{}@bookTicker", TARGET_SYMBOL.to_lowercase());
    loop {
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                info!("✅ Binance Public WebSocket 연결 성공.");
                let (_, mut read) = ws_stream.split();
                while let Some(Ok(msg)) = read.next().await {
                    if let Message::Text(text) = msg {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let (Some(symbol), Some(ask), Some(bid)) = (data["s"].as_str(), data["a"].as_str().and_then(|p| p.parse().ok()), data["b"].as_str().and_then(|p| p.parse().ok())) {
                                let mut prices = app_state.binance_prices.write().await;
                                let entry = prices.entry(symbol.to_string()).or_default();
                                entry.ask_price = ask; entry.bid_price = bid;
                            }
                        }
                    }
                }
            }
            Err(e) => error!("❌ Binance Public WebSocket 연결 오류: {}. 5초 후 재연결...", e),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

// --- Private Stream (역할 변경: 실행 보고서 전달자) ---
async fn bybit_private_stream_task(
    mut order_rx: mpsc::Receiver<OrderRequest>,
    execution_report_tx: mpsc::Sender<ExecutionReport>,
) {
    let url = "wss://stream.bybit.com/v5/trade";
    let mut pending_orders = HashMap::new();

    loop {
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                info!("✅ Bybit Private WebSocket 연결 성공. 인증 시작...");
                let (mut write, mut read) = ws_stream.split();
                
                // 인증 로직
                let expires = (Utc::now().timestamp_millis() + 10000) as u64;
                let signature_payload = format!("GET/realtime{}", expires);
                let mut mac = Hmac::<Sha256>::new_from_slice(BYBIT_API_SECRET.as_bytes()).unwrap();
                mac.update(signature_payload.as_bytes());
                let signature = hex::encode(mac.finalize().into_bytes());
                let auth_msg = json!({"op": "auth", "args": [BYBIT_API_KEY, expires, signature]});
                if write.send(Message::Text(auth_msg.to_string())).await.is_err() {
                    warn!("Bybit 인증 메시지 전송 실패."); continue;
                }

                let (tx, mut rx) = mpsc::channel(1);
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(20)).await;
                        if tx.send(()).await.is_err() { break; }
                    }
                });

                loop {
                    tokio::select! {
                        Some(_) = rx.recv() => {
                            if write.send(Message::Text(json!({"op": "ping"}).to_string())).await.is_err() { error!("Bybit 핑 전송 실패."); break; }
                        }
                        Some(order) = order_rx.recv() => {
                            let req_id = Uuid::new_v4().to_string();
                            pending_orders.insert(req_id.clone(), order.clone());

                            let order_payload = json!({
                                "op": "order.create", "reqId": req_id, "header": {"X-BAPI-TIMESTAMP": Utc::now().timestamp_millis().to_string(), "X-BAPI-RECV-WINDOW": "5000"},
                                "args": [{"category": "linear", "symbol": order.symbol, "side": order.side, "orderType": "Market", "qty": order.qty, "reduceOnly": order.reduce_only }]
                            });
                            if write.send(Message::Text(order_payload.to_string())).await.is_err() {
                                error!("Bybit 주문 전송 실패.");
                                pending_orders.remove(&req_id.clone());
                                break;
                            }
                        }
                        Some(Ok(msg)) = read.next() => {
                             if let Message::Text(text) = msg {
                                if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if resp["op"] == "auth" && resp["retCode"] == 0 { info!("✅ Bybit 인증 완료."); }
                                    else if resp["op"] == "order.create" {
                                        if let Some(req_id) = resp["reqId"].as_str() {
                                            if let Some(original_request) = pending_orders.remove(req_id) {
                                                let report_type = if resp["retCode"] == 0 {
                                                    ReportType::OrderAccepted
                                                } else {
                                                    ReportType::OrderRejected(text.to_string())
                                                };
                                                let report = ExecutionReport { exchange: Exchange::Bybit, report_type, original_request };
                                                if execution_report_tx.send(report).await.is_err() {
                                                    error!("Reconciliation Task로 실행 보고서 전송 실패!");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        else => break,
                    }
                }
            }
            Err(e) => error!("❌ Bybit Private WebSocket 연결 오류: {}. 5초 후 재연결...", e),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn binance_private_stream_task(
    mut order_rx: mpsc::Receiver<OrderRequest>,
    execution_report_tx: mpsc::Sender<ExecutionReport>,
) {
    let url = "wss://ws-fapi.binance.com/ws-fapi/v1";
    let mut pending_orders = HashMap::new();

    loop {
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                info!("✅ Binance Private WebSocket 연결 성공.");
                let (mut write, mut read) = ws_stream.split();
                loop {
                    tokio::select! {
                        Some(order) = order_rx.recv() => {
                            let request_id = Uuid::new_v4().to_string();
                            pending_orders.insert(request_id.clone(), order.clone());

                            let timestamp = Utc::now().timestamp_millis();
                            let mut params = vec![ ("apiKey".to_string(), BINANCE_API_KEY.to_string()), ("symbol".to_string(), order.symbol.clone()), ("side".to_string(), order.side.clone()), ("type".to_string(), "MARKET".to_string()), ("quantity".to_string(), order.qty.clone()), ("timestamp".to_string(), timestamp.to_string()) ];
                            if order.reduce_only { params.push(("reduceOnly".to_string(), "true".to_string())); }
                            params.sort_by(|a, b| a.0.cmp(&b.0));
                            let query_string: String = params.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<String>>().join("&");
                            let mut mac = Hmac::<Sha256>::new_from_slice(BINANCE_API_SECRET.as_bytes()).unwrap();
                            mac.update(query_string.as_bytes());
                            let signature = hex::encode(mac.finalize().into_bytes());
                            let mut params_map: serde_json::Map<String, serde_json::Value> = params.into_iter().map(|(k, v)| (k, serde_json::Value::String(v))).collect();
                            params_map.insert("signature".to_string(), serde_json::Value::String(signature));
                            let request = json!({"id": request_id, "method": "order.place", "params": params_map });
                            if write.send(Message::Text(request.to_string())).await.is_err() {
                                error!("Binance 주문 전송 실패.");
                                pending_orders.remove(&request_id.clone());
                                break;
                            }
                        },
                        Some(Ok(msg)) = read.next() => {
                            if let Message::Text(text) = msg {
                                if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if let Some(id) = resp["id"].as_str() {
                                        if let Some(original_request) = pending_orders.remove(id) {
                                            let report_type = if resp["error"].is_null() && resp["result"].is_object() {
                                                ReportType::OrderAccepted
                                            } else {
                                                ReportType::OrderRejected(text.to_string())
                                            };
                                            let report = ExecutionReport { exchange: Exchange::Binance, report_type, original_request };
                                            if execution_report_tx.send(report).await.is_err() {
                                                error!("Reconciliation Task로 실행 보고서 전송 실패!");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        else => break,
                    }
                }
            }
            Err(e) => error!("❌ Binance Private WebSocket 연결 오류: {}. 5초 후 재연결...", e),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

// --- 신규 추가: 상태 보정 태스크 (Reconciliation Task) ---
async fn reconciliation_task(
    app_state: Arc<AppState>,
    mut report_rx: mpsc::Receiver<ExecutionReport>,
) {
    info!("✅ 상태 보정(Reconciliation) 태스크 시작.");
    while let Some(report) = report_rx.recv().await {
        match report.report_type {
            ReportType::OrderAccepted => {
                // 주문 성공 시, 현재 상태가 낙관적 업데이트와 일치하는지 간단히 확인. (로깅 목적)
                let pos = app_state.position.read().await;
                info!("✅ [상태 확인] {:?} 주문 성공. 현재 상태: {:?}, 진입 횟수: {}", report.exchange, pos.status, pos.entry_count);
            }
            ReportType::OrderRejected(reason) => {
                warn!("🔥 [상태 보정] {:?} 주문 실패! 상태 강제 보정 시작. 사유: {}", report.exchange, reason);
                let mut pos = app_state.position.write().await;
                let req = report.original_request;

                match req.intent {
                    OrderIntent::Enter | OrderIntent::Add => {
                        // 진입/추가 진입 주문이 실패했으므로, 낙관적으로 올렸던 entry_count를 되돌림.
                        warn!("   ➡️ 진입/추가 진입 실패. Entry count 롤백: {} -> {}", pos.entry_count, pos.entry_count.saturating_sub(1));
                        pos.entry_count = pos.entry_count.saturating_sub(1);
                        if pos.entry_count == 0 {
                            pos.status = PositionStatus::None;
                            warn!("   ➡️ Entry count가 0이 되어 포지션 상태를 None으로 초기화.");
                        }
                    }
                    OrderIntent::Close => {
                        // 종료 주문이 실패했으므로, 낙관적으로 초기화했던 상태(None, 0)를 이전 상태로 되돌림.
                        warn!("   ➡️ 종료 실패. 포지션 상태 롤백: {:?} -> {:?}", pos.status, req.position_status_before_action);
                        pos.status = req.position_status_before_action;
                        
                        let base_qty = SIZE.parse::<u32>().unwrap_or(1).max(1);
                        let closed_qty = req.qty.parse::<u32>().unwrap_or(0);
                        let reverted_count = closed_qty / base_qty;
                        warn!("   ➡️ Entry count 롤백: {} -> {}", pos.entry_count, reverted_count);
                        pos.entry_count = reverted_count;
                    }
                }
                 // 여기에 unbalanced position 발생 시 추가적인 위험 관리 로직(예: 강제 청산, 알림)을 구현할 수 있음.
                 error!("🚨 포지션 불일치 발생! 즉각적인 확인 필요! 현재 상태: {:?}, 진입 횟수: {}", pos.status, pos.entry_count);
            }
        }
    }
}


// --- 메인 거래 로직 (역할 변경: 속도 최적화 및 낙관적 업데이트) ---
async fn trading_logic_task(
    app_state: Arc<AppState>,
    bybit_tx: mpsc::Sender<OrderRequest>,
    binance_tx: mpsc::Sender<OrderRequest>,
) -> Result<()> {
    info!("메인 로직 대기 중...");
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("✅ 거래 로직 시작 (낙관적 업데이트 모드)!");

    loop {
        tokio::time::sleep(Duration::from_millis(1)).await;

        let bybit_p = app_state.bybit_prices.read().await;
        let binance_p = app_state.binance_prices.read().await;

        let bybit_data = match bybit_p.get(TARGET_SYMBOL) {
            Some(data) if data.ask_price > 0.0 && data.bid_price > 0.0 => data.clone(),
            _ => continue,
        };
        let binance_data = match binance_p.get(TARGET_SYMBOL) {
            Some(data) if data.ask_price > 0.0 && data.bid_price > 0.0 => data.clone(),
            _ => continue,
        };

        let position_state = *app_state.position.read().await;
        
        drop(bybit_p);
        drop(binance_p);

        let forward_entry_kimp = (bybit_data.ask_price / binance_data.bid_price) * 1000.0;
        let forward_close_kimp = (bybit_data.bid_price / binance_data.ask_price) * 1000.0;
        let reverse_entry_kimp = (binance_data.ask_price / bybit_data.bid_price) * 1000.0;
        let reverse_close_kimp = (binance_data.bid_price / bybit_data.ask_price) * 1000.0;

        match position_state.status {
            PositionStatus::None => {
                if position_state.entry_count < MAX_ENTRY_COUNT {
                    if forward_entry_kimp < ENTRY_GAP {
                        let bybit_order = OrderRequest { symbol: TARGET_SYMBOL.to_string(), side: "Buy".to_string(), qty: SIZE.to_string(), reduce_only: false, intent: OrderIntent::Enter, position_status_before_action: position_state.status };
                        let binance_order = OrderRequest { symbol: TARGET_SYMBOL.to_string(), side: "SELL".to_string(), qty: SIZE.to_string(), reduce_only: false, intent: OrderIntent::Enter, position_status_before_action: position_state.status };
                        
                        // 주문 전송 후 응답을 기다리지 않음 (Fire-and-forget)
                        bybit_tx.send(bybit_order).await.ok();
                        binance_tx.send(binance_order).await.ok();
                        
                        // ** 낙관적 상태 업데이트 (Optimistic Update) **
                        let mut pos = app_state.position.write().await;
                        pos.status = PositionStatus::BybitLong;
                        pos.entry_count += 1;
                        info!("⬇️ [정방향 진입] 시도 후 즉시 상태 변경 (낙관적): BybitLong, 진입 횟수: {}", pos.entry_count);

                    } else if reverse_entry_kimp < ENTRY_GAP {
                        let binance_order = OrderRequest { symbol: TARGET_SYMBOL.to_string(), side: "BUY".to_string(), qty: SIZE.to_string(), reduce_only: false, intent: OrderIntent::Enter, position_status_before_action: position_state.status };
                        let bybit_order = OrderRequest { symbol: TARGET_SYMBOL.to_string(), side: "Sell".to_string(), qty: SIZE.to_string(), reduce_only: false, intent: OrderIntent::Enter, position_status_before_action: position_state.status };
                        
                        binance_tx.send(binance_order).await.ok();
                        bybit_tx.send(bybit_order).await.ok();
                        
                        let mut pos = app_state.position.write().await;
                        pos.status = PositionStatus::BinanceLong;
                        pos.entry_count += 1;
                        info!("⬇️ [역방향 진입] 시도 후 즉시 상태 변경 (낙관적): BinanceLong, 진입 횟수: {}", pos.entry_count);
                    }
                }
            }
            PositionStatus::BybitLong => {
                if forward_entry_kimp < ENTRY_GAP && position_state.entry_count < MAX_ENTRY_COUNT {
                    let bybit_order = OrderRequest { symbol: TARGET_SYMBOL.to_string(), side: "Buy".to_string(), qty: SIZE.to_string(), reduce_only: false, intent: OrderIntent::Add, position_status_before_action: position_state.status };
                    let binance_order = OrderRequest { symbol: TARGET_SYMBOL.to_string(), side: "SELL".to_string(), qty: SIZE.to_string(), reduce_only: false, intent: OrderIntent::Add, position_status_before_action: position_state.status };
                    
                    bybit_tx.send(bybit_order).await.ok();
                    binance_tx.send(binance_order).await.ok();

                    let mut pos = app_state.position.write().await;
                    pos.entry_count += 1;
                    info!("⬇️ [정방향 추가 진입] 시도 후 즉시 상태 변경 (낙관적), 진입 횟수: {}", pos.entry_count);

                } else if forward_close_kimp > TARGET_GAP && position_state.entry_count >= 1 {
                    let close_qty = (SIZE.parse::<u32>().unwrap_or(0) * position_state.entry_count).to_string();
                    info!("⬆️ [정방향 종료] 시도 - 갭: {:.2}, 수량: {}", forward_close_kimp, close_qty);
                    
                    let bybit_order = OrderRequest { symbol: TARGET_SYMBOL.to_string(), side: "Sell".to_string(), qty: close_qty.clone(), reduce_only: true, intent: OrderIntent::Close, position_status_before_action: position_state.status };
                    let binance_order = OrderRequest { symbol: TARGET_SYMBOL.to_string(), side: "BUY".to_string(), qty: close_qty, reduce_only: true, intent: OrderIntent::Close, position_status_before_action: position_state.status };
                    
                    bybit_tx.send(bybit_order).await.ok();
                    binance_tx.send(binance_order).await.ok();
                    
                    let mut pos = app_state.position.write().await;
                    pos.status = PositionStatus::None;
                    pos.entry_count = 0;
                    info!("   ➡️ [종료] 시도 후 즉시 상태 초기화 (낙관적): None");
                }
            }
            PositionStatus::BinanceLong => {
                if reverse_entry_kimp < ENTRY_GAP && position_state.entry_count < MAX_ENTRY_COUNT {
                    let binance_order = OrderRequest { symbol: TARGET_SYMBOL.to_string(), side: "BUY".to_string(), qty: SIZE.to_string(), reduce_only: false, intent: OrderIntent::Add, position_status_before_action: position_state.status };
                    let bybit_order = OrderRequest { symbol: TARGET_SYMBOL.to_string(), side: "Sell".to_string(), qty: SIZE.to_string(), reduce_only: false, intent: OrderIntent::Add, position_status_before_action: position_state.status };
                    
                    binance_tx.send(binance_order).await.ok();
                    bybit_tx.send(bybit_order).await.ok();
                    
                    let mut pos = app_state.position.write().await;
                    pos.entry_count += 1;
                    info!("⬇️ [역방향 추가 진입] 시도 후 즉시 상태 변경 (낙관적), 진입 횟수: {}", pos.entry_count);

                } else if reverse_close_kimp > TARGET_GAP && position_state.entry_count >= 1 {
                    let close_qty = (SIZE.parse::<u32>().unwrap_or(0) * position_state.entry_count).to_string();
                    info!("⬆️ [역방향 종료] 시도 - 갭: {:.2}, 수량: {}", reverse_close_kimp, close_qty);

                    let binance_order = OrderRequest { symbol: TARGET_SYMBOL.to_string(), side: "SELL".to_string(), qty: close_qty.clone(), reduce_only: true, intent: OrderIntent::Close, position_status_before_action: position_state.status };
                    let bybit_order = OrderRequest { symbol: TARGET_SYMBOL.to_string(), side: "Buy".to_string(), qty: close_qty, reduce_only: true, intent: OrderIntent::Close, position_status_before_action: position_state.status };
                    
                    binance_tx.send(binance_order).await.ok();
                    bybit_tx.send(bybit_order).await.ok();
                    
                    let mut pos = app_state.position.write().await;
                    pos.status = PositionStatus::None;
                    pos.entry_count = 0;
                    info!("   ➡️ [종료] 시도 후 즉시 상태 초기화 (낙관적): None");
                }
            }
        }
    }
}

// --- 메인 함수 (신규 아키텍처 적용) ---
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    if BYBIT_API_KEY.contains("YOUR") || BINANCE_API_KEY.contains("YOUR") {
        warn!("!!! 경고: API 키와 시크릿을 실제 값으로 변경해주세요. !!!");
        return Ok(());
    }

    let app_state = Arc::new(AppState {
        bybit_prices: RwLock::new(HashMap::new()),
        binance_prices: RwLock::new(HashMap::new()),
        position: RwLock::new(PositionState {
            status: PositionStatus::None,
            entry_count: 0,
        }),
    });

    initialize_data(&app_state).await?;

    // 주문 및 실행 보고를 위한 MPSC 채널 생성
    let (bybit_tx, bybit_rx) = mpsc::channel(100);
    let (binance_tx, binance_rx) = mpsc::channel(100);
    let (execution_report_tx, execution_report_rx) = mpsc::channel(100);

    // 각 태스크에 필요한 채널과 상태 클론
    let bybit_exec_tx = execution_report_tx.clone();
    let binance_exec_tx = execution_report_tx.clone();
    let state_for_recon = Arc::clone(&app_state);

    // 태스크 실행
    let bybit_public_handle = tokio::spawn(bybit_public_stream_task(Arc::clone(&app_state)));
    let binance_public_handle = tokio::spawn(binance_public_stream_task(Arc::clone(&app_state)));
    let bybit_private_handle = tokio::spawn(bybit_private_stream_task(bybit_rx, bybit_exec_tx));
    let binance_private_handle = tokio::spawn(binance_private_stream_task(binance_rx, binance_exec_tx));
    let logic_handle = tokio::spawn(trading_logic_task(Arc::clone(&app_state), bybit_tx, binance_tx));
    // 신규: 상태 보정 태스크 실행
    let reconciliation_handle = tokio::spawn(reconciliation_task(state_for_recon, execution_report_rx));

    let _ = tokio::try_join!(
        bybit_public_handle, 
        binance_public_handle, 
        bybit_private_handle, 
        binance_private_handle, 
        logic_handle,
        reconciliation_handle
    )?;

    Ok(())
}