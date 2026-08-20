const ctx = document.getElementById('LatencyChart').getContext('2d');
        let eventMetadata = [];

        const Latency_Chart = new Chart(ctx, {
            type: 'line',

            data: {
                labels: [],
                datasets: [{
                    label: 'System Communication Latency',
                    data: [],
                    borderColor: 'rgba(75, 192, 192, 1)',
                    borderWidth: 2,
                    backgroundColor: 'rgba(75, 192, 192, 0.2)'
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
                            font: { size: 16 },
                            color: '#000'
                        }
                    },

                    tooltip: {
                        callbacks: {

                            title: function(context) {
                                const index = context[0].dataIndex;
                                const meta = eventMetadata[index];

                                if (!meta) {
                                    return context[0].label;
                                }

                                return meta.station;
                            },

                            label: function(context) {
                                const index = context.dataIndex;
                                const meta = eventMetadata[index];

                                if (!meta) {
                                    return `Latency: ${context.parsed.y} ms`;
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
                                type: 'label',

                                xScaleID: 'x',
                                yScaleID: 'x',

                                xValue: 0,
                                yValue: 0,

                                //Adjust label position
                                xAdjust: 890,
                                yAdjust: 440,

                                content: ['Newest', 'Record'],
                                backgroundColor: 'rgba(255, 255, 255, 0.9)',
                                color: '#000',
                                font: { size: 12 },

                                textAlign: 'center'
                            },
                            oldestMessageLabel: {
                                type: 'label',

                                xScaleID: 'x',
                                yScaleID: 'x',

                                xValue: 0,
                                yValue: 0,

                                xAdjust: 0,
                                yAdjust: 440,

                                content: ['Oldest', 'Record'],
                                backgroundColor: 'rgba(255, 255, 255, 0.9)',
                                color: '#000000',
                                font: { size: 12 },

                                textAlign: 'center'
                            },
                        }
                    }
                },

                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Event Source and Time Received',
                            font: { size: 14 },
                            color: '#000'
                        },

                        ticks: {
                            font: { size: 13 }
                        }
                    },

                    y: {
                        beginAtZero: true,

                        title: {
                            display: true,
                            text: 'Latency (ms)',
                            font: { size: 14 },
                            color: '#000'
                        },

                        ticks: {
                            font: { size: 13 }
                        }
                    }
                }
            }
        });
        // Link to the URL where the SSEs come from
        const url = window.Latency_SSE_URL;
        const source = new EventSource(url)

        source.onmessage = function(event) {
            const data = JSON.parse(event.data);

            eventMetadata = data.event_metadata_array;

            Latency_Chart.data.labels = data.event_source_array;
            Latency_Chart.data.datasets[0].data = data.latency_array;

            Latency_Chart.update();
        };

        // const source = new EventSource("{% url 'latency_graphing' %}");
        // source.onmessage = function (event) {
        //     const data = JSON.parse(event.data);//receive the JSON blob

        //     let messageDateTime_Array = [...data.time_sent_array];//create new arrays and add the received data to them            
        //     let messageLatency_Array = [...data.latency_array];
        //     let numberReceivedMessages = messageDateTime_Array.length


        //     let numberMessagesDisplayed = Latency_Chart.data.labels.length;

        //     if (numberMessagesDisplayed == 0) {
        //         for(let index=numberReceivedMessages; index > 0 ; index--)
        //         {
        //             Latency_Chart.data.labels.push(messageDateTime_Array[index-1]);//push formatted time to the chart (X Axis)
        //             Latency_Chart.data.datasets[0].data.push(messageLatency_Array[index-1]);//push latency (Y Axis)
        //         }
        //     }
        //     else if (numberMessagesDisplayed != 0) {

        //             //ensure empty messages can't be added
        //             if((messageDateTime_Array[0]!=null)&&(messageLatency_Array[0]!=null)){
        //                 lastDateTime = Latency_Chart.data.labels[numberMessagesDisplayed-1];//get the last time
        //                 lastDateTime_String = lastDateTime.join(" ");//combine array into a string for comparison
        //                 newDateTime = messageDateTime_Array[0].join(" ");//get new data from the array

        //                 if (lastDateTime_String != newDateTime)//only add the data if it is new
        //                 {
        //                     // Removes oldest data point
        //                     Latency_Chart.data.labels.shift();
        //                     Latency_Chart.data.datasets[0].data.shift();

        //                     //push formatted time to the chart
        //                     Latency_Chart.data.labels.push(messageDateTime_Array[0]);
        //                     Latency_Chart.data.datasets[0].data.push(messageLatency_Array[0]);
        //                 }
        //             }
        //     }
        //      Latency_Chart.update();
        // };