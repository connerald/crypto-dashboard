const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

function loadDashboard() {
    const html = fs.readFileSync(
        path.join(__dirname, '../crypto-dashboard/templates/index.html'), 'utf8'
    );
    const handlers = {};
    const points = [];
    const priceElement = { innerText: '--' };
    const context = {
        Highcharts: {
            chart: () => ({
                series: [{ data: [], addPoint: point => points.push(point) }]
            })
        },
        io: () => ({ on: (event, handler) => { handlers[event] = handler; } }),
        document: { getElementById: () => priceElement },
        console: { log() {} },
        setTimeout() {},
        // Reading reception time must never be necessary to plot a trade.
        Date: class { constructor() { throw new Error('Browser clock used'); } }
    };
    for (const match of html.matchAll(/<script>([\s\S]*?)<\/script>/g)) {
        vm.runInNewContext(match[1], context);
    }
    return { onPrice: handlers.btc_price, points, priceElement };
}

test('delayed price is plotted at its original trade timestamp', () => {
    const { onPrice, points, priceElement } = loadDashboard();
    onPrice({ price: 67234.5, ts: 1720000000123 });
    assert.equal(points.length, 1);
    assert.equal(points[0][0], 1720000000123);
    assert.equal(points[0][1], 67234.5);
    assert.equal(priceElement.innerText, 67234.5);
});

test('invalid timestamps do not create points or replace the displayed price', () => {
    const { onPrice, points, priceElement } = loadDashboard();
    for (const ts of [undefined, null, '1720000000123', 0, -1, NaN, Infinity, 1.5]) {
        onPrice({ price: 67234.5, ts });
    }
    assert.equal(points.length, 0);
    assert.equal(priceElement.innerText, '--');
    onPrice({ price: 67235, ts: 1720000002123 });
    assert.equal(points.length, 1);
});
