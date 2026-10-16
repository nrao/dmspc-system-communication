function dispatchRadarEvent(
    browserEventName,
    event
) {
    let data = {};

    try {
        data = JSON.parse(
            event.data || "{}"
        );
    } catch (error) {
        console.error(
            `[SSE] Could not parse ${event.type}:`,
            error
        );

        return;
    }

    console.log(
        `[SSE] ${event.type}:`,
        data
    );

    document.body.dispatchEvent(
        new CustomEvent(
            browserEventName,
            {
                detail: data
            }
        )
    );
}

const HOME_EVENTS = {
    gbt_changed:
        "gbtChanged",

    vlba_changed:
        "vlbaChanged",

    dsoc_changed:
        "dsocChanged",

    progress_changed:
        "progressChanged",

    image_changed:
        "imageChanged",

};



for (
    const [
        sseEvent,
        browserEvent
    ] of Object.entries(HOME_EVENTS)
) {
    window.eventSource.addEventListener(
        sseEvent,
        (event) => {
            dispatchRadarEvent(
                browserEvent,
                event
            );
        }
    );
}