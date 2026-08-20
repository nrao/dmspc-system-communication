function getTime() {
            const now = new Date();
            const year = now.getUTCFullYear();
            const month = String(now.getUTCMonth() + 1).padStart(2, '0');
            const day = String(now.getUTCDate()).padStart(2, '0');
            const hours = String(now.getUTCHours()).padStart(2, '0');
            const minutes = String(now.getUTCMinutes()).padStart(2, '0');
            const seconds = String(now.getUTCSeconds()).padStart(2, '0');
            const formattedTimestamp = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
            document.getElementById('live-clock').textContent = formattedTimestamp;
        }
        getTime();
        setInterval(getTime, 1000);