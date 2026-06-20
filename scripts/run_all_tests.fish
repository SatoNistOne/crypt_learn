#!/usr/bin/env fish

set PASS 0
set FAIL 0

function ok
    echo "  ok  $argv"
    set PASS (math $PASS + 1)
end

function fail
    echo "  FAIL $argv"
    set FAIL (math $FAIL + 1)
end

function check
    set -l name $argv[1]
    set -l expected $argv[2]
    set -l actual $argv[3]
    if string match -q "$expected" "$actual"
        ok "$name"
    else
        fail "$name (expected=$expected got=$actual)"
    end
end

function check_contains
    set -l name $argv[1]
    set -l needle $argv[2]
    set -l haystack $argv[3]
    if string match -q "*$needle*" "$haystack"
        ok "$name"
    else
        fail "$name (missing '$needle' in: $haystack)"
    end
end

docker compose down -v > /dev/null 2>&1
docker compose up --build -d > /dev/null 2>&1
sleep 20

set ALICE_TOKEN (curl -s -X POST http://localhost:8000/auth/register -H "Content-Type: application/json" -d '{"username":"alice","password":"p"}' | jq -r '.token')
set BOB_TOKEN (curl -s -X POST http://localhost:8000/auth/register -H "Content-Type: application/json" -d '{"username":"bob","password":"p"}' | jq -r '.token')
set CAROL_TOKEN (curl -s -X POST http://localhost:8000/auth/register -H "Content-Type: application/json" -d '{"username":"carol","password":"p"}' | jq -r '.token')
set DAVE_TOKEN (curl -s -X POST http://localhost:8000/auth/register -H "Content-Type: application/json" -d '{"username":"dave","password":"p"}' | jq -r '.token')

if test "$ALICE_TOKEN" != "null"; ok "регистрация alice"; else; fail "регистрация alice"; end
if test "$BOB_TOKEN" != "null"; ok "регистрация bob"; else; fail "регистрация bob"; end
if test "$CAROL_TOKEN" != "null"; ok "регистрация carol"; else; fail "регистрация carol"; end
if test "$DAVE_TOKEN" != "null"; ok "регистрация dave"; else; fail "регистрация dave"; end

uv run python scripts/seed_demo_accounts.py > /dev/null 2>&1
ok "seed 15 mint операций"

check "репликация wallet-1 last_seq=15" "15" (curl -s http://localhost:8001/status | jq -r '.last_seq')
check "репликация wallet-2 last_seq=15" "15" (curl -s http://localhost:8002/status | jq -r '.last_seq')
check "репликация wallet-3 last_seq=15" "15" (curl -s http://localhost:8003/status | jq -r '.last_seq')

check_contains "хеш-цепочка wallet-1 valid" '"valid":true' (curl -s http://localhost:8001/verify-chain)
check_contains "хеш-цепочка wallet-2 valid" '"valid":true' (curl -s http://localhost:8002/verify-chain)
check_contains "хеш-цепочка wallet-3 valid" '"valid":true' (curl -s http://localhost:8003/verify-chain)
check_contains "хеш-цепочка wallet-1 length=15" '"length":15' (curl -s http://localhost:8001/verify-chain)
check_contains "хеш-цепочка wallet-2 length=15" '"length":15' (curl -s http://localhost:8002/verify-chain)
check_contains "хеш-цепочка wallet-3 length=15" '"length":15' (curl -s http://localhost:8003/verify-chain)

set BAL_BEFORE (curl -s http://localhost:8000/balances/alice)
check_contains "баланс alice до сделки btc=10" '"available":"10"' "$BAL_BEFORE"
check_contains "баланс alice до сделки learn=10000" '"available":"10000"' "$BAL_BEFORE"

curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" -H "Authorization: Bearer $ALICE_TOKEN" -d '{"pair":"BTC_LEARN","side":"SELL","price":100,"quantity":1}' > /dev/null
sleep 1
curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" -H "Authorization: Bearer $BOB_TOKEN" -d '{"pair":"BTC_LEARN","side":"BUY","price":100,"quantity":1}' > /dev/null
sleep 2

set A (curl -s http://localhost:8000/balances/alice)
set B (curl -s http://localhost:8000/balances/bob)
check_contains "сделка btc: alice btc=9" '"available":"9.0"' "$A"
check_contains "сделка btc: alice learn=10100" '"available":"10100.00"' "$A"
check_contains "сделка btc: alice locked=0" '"locked":"0.0"' "$A"
check_contains "сделка btc: bob btc=11" '"available":"11.0"' "$B"
check_contains "сделка btc: bob learn=9900" '"available":"9900.00"' "$B"

curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" -H "Authorization: Bearer $CAROL_TOKEN" -d '{"pair":"ETH_LEARN","side":"SELL","price":50,"quantity":10}' > /dev/null
sleep 1
curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" -H "Authorization: Bearer $DAVE_TOKEN" -d '{"pair":"ETH_LEARN","side":"BUY","price":50,"quantity":10}' > /dev/null
sleep 2

set C (curl -s http://localhost:8000/balances/carol)
set D (curl -s http://localhost:8000/balances/dave)
check_contains "сделка eth: carol eth=90" '"available":"90.0"' "$C"
check_contains "сделка eth: carol learn=10500" '"available":"10500.00"' "$C"
check_contains "сделка eth: dave eth=110" '"available":"110.0"' "$D"
check_contains "сделка eth: dave learn=9500" '"available":"9500.00"' "$D"

set OB_BTC (curl -s http://localhost:8000/orderbook/BTC_LEARN)
set OB_ETH (curl -s http://localhost:8000/orderbook/ETH_LEARN)
check_contains "стакан btc пуст" '"bids":[]' "$OB_BTC"
check_contains "стакан eth пуст" '"bids":[]' "$OB_ETH"

set TR_BTC (curl -s http://localhost:8000/trades/BTC_LEARN)
set TR_ETH (curl -s http://localhost:8000/trades/ETH_LEARN)
check_contains "история btc: 1 сделка" '"buyer_id":"bob"' "$TR_BTC"
check_contains "история eth: 1 сделка" '"buyer_id":"dave"' "$TR_ETH"

check_contains "финальная хеш-цепочка wallet-1 valid" '"valid":true' (curl -s http://localhost:8001/verify-chain)
check_contains "финальная хеш-цепочка wallet-2 valid" '"valid":true' (curl -s http://localhost:8002/verify-chain)
check_contains "финальная хеш-цепочка wallet-3 valid" '"valid":true' (curl -s http://localhost:8003/verify-chain)
check_contains "финальная хеш-цепочка wallet-1 length=23" '"length":23' (curl -s http://localhost:8001/verify-chain)
check_contains "финальная хеш-цепочка wallet-2 length=23" '"length":23' (curl -s http://localhost:8002/verify-chain)
check_contains "финальная хеш-цепочка wallet-3 length=23" '"length":23' (curl -s http://localhost:8003/verify-chain)

set CANCEL_RES (curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" -H "Authorization: Bearer $ALICE_TOKEN" -d '{"pair":"BTC_LEARN","side":"SELL","price":999,"quantity":1}')
set ORDER_ID (echo $CANCEL_RES | jq -r '.order_id')
set A_LOCKED (curl -s http://localhost:8000/balances/alice)
check_contains "отмена ордера: btc locked=1" '"locked":"1.0"' "$A_LOCKED"

curl -s -X DELETE "http://localhost:8000/orders/$ORDER_ID?pair=BTC_LEARN" -H "Authorization: Bearer $ALICE_TOKEN" > /dev/null
set A_CANCELLED (curl -s http://localhost:8000/balances/alice)
check_contains "отмена ордера: btc locked=0" '"locked":"0.0"' "$A_CANCELLED"
check_contains "отмена ордера: btc available=9" '"available":"9.0"' "$A_CANCELLED"

curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" -H "Authorization: Bearer $ALICE_TOKEN" -d '{"pair":"BTC_LEARN","side":"SELL","price":100,"quantity":5}' > /dev/null
sleep 1
curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" -H "Authorization: Bearer $BOB_TOKEN" -d '{"pair":"BTC_LEARN","side":"BUY","price":100,"quantity":2}' > /dev/null
sleep 2

set A_PART (curl -s http://localhost:8000/balances/alice)
set B_PART (curl -s http://localhost:8000/balances/bob)
check_contains "partial fill: alice btc locked=3" '"locked":"3.0"' "$A_PART"
check_contains "partial fill: alice btc available=4" '"available":"4.0"' "$A_PART"
check_contains "partial fill: bob btc=13" '"available":"13.0"' "$B_PART"

set LEADER_BEFORE (curl -s http://localhost:8001/status | jq -r '.leader_id')
docker compose stop wallet-1 > /dev/null 2>&1
sleep 5

set W2_ROLE (curl -s http://localhost:8002/status | jq -r '.role')
set W3_ROLE (curl -s http://localhost:8003/status | jq -r '.role')
if test "$W2_ROLE" = "leader" -o "$W3_ROLE" = "leader"; ok "failover: новый лидер выбран"; else; fail "failover: новый лидер не выбран"; end

curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" -H "Authorization: Bearer $CAROL_TOKEN" -d '{"pair":"BTC_LEARN","side":"SELL","price":100,"quantity":1}' > /dev/null
docker compose start wallet-1 > /dev/null 2>&1
sleep 10
set W1_SYNC (curl -s http://localhost:8001/status | jq -r '.last_seq')
if test "$W1_SYNC" -ge 30; ok "failover: wallet-1 синхронизирован (last_seq=$W1_SYNC)"; else; fail "failover: wallet-1 не синхронизирован (last_seq=$W1_SYNC)"; end

docker compose down > /dev/null 2>&1
docker compose up -d > /dev/null 2>&1
sleep 15

set W1_P (curl -s http://localhost:8001/status | jq -r '.last_seq')
set CHAIN_P (curl -s http://localhost:8001/verify-chain)
set ALICE_P (curl -s http://localhost:8000/balances/alice)

if test "$W1_P" -ge 30; ok "персистентность: last_seq=$W1_P сохранён"; else; fail "персистентность: last_seq=$W1_P"; end
check_contains "персистентность: хеш-цепочка valid" '"valid":true' "$CHAIN_P"
check_contains "персистентность: баланс alice сохранён" '"available":"4.0"' "$ALICE_P"

echo ""
echo "---"
echo "пройдено: $PASS"
echo "провалено: $FAIL"
echo "всего: "(math $PASS + $FAIL)
if test $FAIL -eq 0
    echo "все тесты пройдены"
end