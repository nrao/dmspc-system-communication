window.eventSource =
    new EventSource(
        "/events/stream/"
    );

window.eventSource.onopen = () => {
    console.log(
        "[SSE] Connection opened"
    );
};

window.eventSource.onerror = (
    error
) => {
    console.error(
        "[SSE] Connection error:",
        error
    );
};

window.eventSource.addEventListener(
    "heartbeat",
    (event) => {
        try {
            const data =
                JSON.parse(
                    event.data
                );

            console.log(
                "[SSE] Heartbeat:",
                data
            );
        } catch (error) {
            console.error(
                "[SSE] Could not parse heartbeat:",
                error
            );
        }
    }
);

window.addEventListener(
    "pagehide",
    () => {
        if (window.eventSource) {
            window.eventSource.close();
        }
    }
);