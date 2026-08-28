(() => {
    const receivedLabel = document.getElementById("bytesText");
    const bar = document.getElementById("bar");
    const statusText = document.getElementById("statusText");

    let es = null;
    let lastTransferId = null;

    function resetUI() {
        bar.style.width = `0%`;
        statusText.textContent = `0%`;
        receivedLabel.textContent = `received: 0 / total: 0`;
    }

    function startSSE() {
        if (es) return;

        es = new EventSource("/progress/stream/");

        es.onmessage = (e) => {
            const data = JSON.parse(e.data);

            const transferId = data.transfer_id ?? null;
            if (transferId !== null && transferId !== lastTransferId) {
                lastTransferId = transferId;
                resetUI();
            }

            const received = data.received ?? 0;
            const total = data.total ?? 0;
            const percent = data.percent ?? 0;

            const safePercent = Math.max(0, Math.min(100, percent));
            bar.style.width = `${safePercent.toFixed(2)}%`;
            statusText.textContent = total > 0 ? `${safePercent.toFixed(1)}%` : "—";
            receivedLabel.textContent =
                `received: ${received} / total: ${total}`;
        };

        es.addEventListener("done", (e) => {
            const data = JSON.parse(e.data);
            const percent = data.percent ?? 100;

            bar.style.width = `${Math.max(0, Math.min(100, percent)).toFixed(2)}%`;
            statusText.textContent = "100%";
        });

        es.onerror = () => {
            // EventSource may reconnect automatically, but we can be explicit
            try { es && es.close(); } catch {}
            es = null;

            setTimeout(() => startSSE(), 1000);
        };
    }

    resetUI();
    startSSE();
})(); 