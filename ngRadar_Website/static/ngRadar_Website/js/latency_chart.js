const canvas =
    document.getElementById(
        "LatencyChart"
    );

if (!canvas) {
    console.warn(
        "[Latency] Chart canvas not found."
    );
} else {
    const ctx =
        canvas.getContext("2d");

    let eventMetadata = [];

    const LatencyChart =
        new Chart(
            ctx,
            {
                type: "line",

                data: {
                    labels: [],

                    datasets: [{
                        label:
                            "System Communication Latency",

                        data: [],

                        borderColor:
                            "rgba(75, 192, 192, 1)",

                        borderWidth: 2,

                        backgroundColor:
                            "rgba(75, 192, 192, 0.2)"
                    }]
                },

                options: {
                    responsive: false,
                    clip: false,

                    layout: {
                        padding: {
                            bottom: 20
                        }
                    },

                    plugins: {
                        legend: {
                            labels: {
                                font: {
                                    size: 16
                                },

                                color: "#000"
                            }
                        },

                        tooltip: {
                            callbacks: {
                                title(context) {
                                    const index =
                                        context[0]
                                            .dataIndex;

                                    const meta =
                                        eventMetadata[
                                            index
                                        ];

                                    if (!meta) {
                                        return context[
                                            0
                                        ].label;
                                    }

                                    return meta.station;
                                },

                                label(context) {
                                    const index =
                                        context.dataIndex;

                                    const meta =
                                        eventMetadata[
                                            index
                                        ];

                                    if (!meta) {
                                        return (
                                            `Latency: `
                                            + `${context.parsed.y} ms`
                                        );
                                    }

                                    return [
                                        `Status: ${meta.status}`,
                                        `Latency: ${context.parsed.y} ms`,
                                        `Time: ${meta.time}`,
                                        `Object ID: ${meta.object_id}`,
                                        `Target: ${meta.target}`
                                    ];
                                }
                            }
                        },

                        annotation: {
                            annotations: {
                                newestMessageLabel: {
                                    type: "label",
                                    xScaleID: "x",
                                    yScaleID: "x",
                                    xValue: 0,
                                    yValue: 0,
                                    xAdjust: 890,
                                    yAdjust: 440,

                                    content: [
                                        "Newest",
                                        "Event"
                                    ],

                                    backgroundColor:
                                        "rgba(255, 255, 255, 0.9)",

                                    color: "#000",

                                    font: {
                                        size: 12
                                    },

                                    textAlign:
                                        "center"
                                },

                                oldestMessageLabel: {
                                    type: "label",
                                    xScaleID: "x",
                                    yScaleID: "x",
                                    xValue: 0,
                                    yValue: 0,
                                    xAdjust: 0,
                                    yAdjust: 440,

                                    content: [
                                        "Oldest",
                                        "Event"
                                    ],

                                    backgroundColor:
                                        "rgba(255, 255, 255, 0.9)",

                                    color: "#000",

                                    font: {
                                        size: 12
                                    },

                                    textAlign:
                                        "center"
                                }
                            }
                        }
                    },

                    scales: {
                        x: {
                            title: {
                                display: true,

                                text:
                                    "Event Source and Time Received",

                                font: {
                                    size: 14
                                },

                                color: "#000"
                            },

                            ticks: {
                                font: {
                                    size: 13
                                }
                            }
                        },

                        y: {
                            beginAtZero: true,

                            title: {
                                display: true,

                                text:
                                    "Latency (ms)",

                                font: {
                                    size: 14
                                },

                                color: "#000"
                            },

                            ticks: {
                                font: {
                                    size: 13
                                }
                            }
                        }
                    }
                }
            }
        );


    async function refreshLatencyChart() {
        try {
            const response =
                await fetch(
                    window.LATENCY_DATA_URL
                );

            if (!response.ok) {
                throw new Error(
                    `HTTP ${response.status}`
                );
            }

            const data =
                await response.json();

            eventMetadata =
                data.event_metadata_array;

            LatencyChart.data.labels =
                data.event_source_array;

            LatencyChart
                .data
                .datasets[0]
                .data =
                    data.latency_array;

            LatencyChart.update();

            console.log(
                "[Latency] Chart refreshed."
            );

        } catch (error) {
            console.error(
                "[Latency] Could not "
                + "refresh chart:",
                error
            );
        }
    }


    document.body.addEventListener(
        "observatoryEventCreated",
        refreshLatencyChart
    );


    // Initial persisted data.
    refreshLatencyChart();
}