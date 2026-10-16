window.eventSource.addEventListener(
    "observatory_event_created",
    (event) => {
        let data;

        try {
            data = JSON.parse(
                event.data
            );
        } catch (error) {
            console.error(
                "[SSE] Could not parse "
                + "observatory event:",
                error
            );
            return;
        }

        console.log(
            "[SSE] Observatory event created:",
            data
        );

        document.body.dispatchEvent(
            new CustomEvent(
                "observatoryEventCreated",
                {
                    detail: data
                }
            )
        );
    }
);