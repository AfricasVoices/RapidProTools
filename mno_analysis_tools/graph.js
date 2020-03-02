// Set the dimensions and margins of the graph
const margin = { top: 20, right: 30, bottom: 70, left: 50 },
    graphWidth = 960 - margin.left - margin.right,
    graphHeight = 500 - margin.top - margin.bottom;

// Incoming Messages
// Svg
const incomingMsgGraphSvg = d3
        .select(".incoming-graph")
        .append("svg")
        .attr("width", graphWidth + margin.left + margin.right)
        .attr("height", graphHeight + margin.top + margin.bottom),
    // Graph
    incomingMsgGraph = incomingMsgGraphSvg
        .append("g")
        .attr("width", graphWidth)
        .attr("Height", graphHeight)
        .attr("transform", `translate(${margin.left}, ${margin.top})`),
    // Scales
    x = d3.scaleTime().range([0, graphWidth]),
    y = d3.scaleLinear().range([graphHeight, 0]),
    yRight = d3.scaleLinear().range([graphHeight, 0]),
    // Axes group
    incomingMsgGraphxAxisGroup = incomingMsgGraph
        .append("g")
        .attr("class", "x-axis")
        .attr("transform", "translate(0," + graphHeight + ")"),
    incomingMsgGraphyAxisGroup = incomingMsgGraph.append("g").attr("class", "y-axis"),
    yAxisGroup2 = incomingMsgGraph
        .append("g")
        .attr("transform", "translate( " + graphWidth + ", 0 )")
        .attr("class", "y-axis2"),
    // Line
    MessageDifferenceLine = d3
        .line()
        .x(d => x(new Date(d.PeriodBetween)))
        .y(d => yRight(d.MessageDifference)),
    NumberOfMessagesLine = d3
        .line()
        .x(d => x(new Date(d.PeriodEnd)))
        .y(d => yRight(d.NumberOfMessages)),
    // d3 line path generator
    path1 = incomingMsgGraph.append("path"),
    path = incomingMsgGraph.append("path");

// Outgoing Messages
// Svg
const outgoingMsgGraphSvg = d3
        .select(".outgoing-graph")
        .append("svg")
        .attr("width", graphWidth + margin.left + margin.right)
        .attr("height", graphHeight + margin.top + margin.bottom),
    // Graph
    outgoingMsgGraph = outgoingMsgGraphSvg
        .append("g")
        .attr("width", graphWidth)
        .attr("Height", graphHeight)
        .attr("transform", `translate(${margin.left}, ${margin.top})`),
    // Scales
    outgoingMsgGraphxScale = d3.scaleTime().range([0, graphWidth]),
    outgoingMsgGraphyScale = d3.scaleLinear().range([graphHeight, 0]),
    outgoingMsgGraphyRightScale = d3.scaleLinear().range([graphHeight, 0]),
    // Axes groups
    outgoingMsgGraphxAxisGroup = outgoingMsgGraph
        .append("g")
        .attr("class", "x-axis")
        .attr("transform", "translate(0," + graphHeight + ")"),
    outgoingMsgGraphRightYAxisGroup = outgoingMsgGraph.append("g").attr("class", "right-y-axis"),
    outgoingMsgGraphLeftYAxisGroup = outgoingMsgGraph
        .append("g")
        .attr("transform", "translate( " + graphWidth + ", 0 )")
        .attr("class", "left-y-axis"),
    // Line
    outgoingMsgGraphMessageDifferenceLine = d3
        .line()
        .x(d => x(new Date(d.PeriodBetween)))
        .y(d => yRight(d.MessageDifference)),
    outgoingMsgGraphNumberOfMessagesLine = d3
        .line()
        .x(d => x(new Date(d.PeriodEnd)))
        .y(d => yRight(d.NumberOfMessages)),
    // d3 line path generator
    outgoingMsgGraphPath1 = outgoingMsgGraph.append("path"),
    outgoingMsgGraphPath = outgoingMsgGraph.append("path");

Promise.all([
    d3.json("incoming_downtime.json"),
    d3.json("outgoing_downtime.json"),
    d3.json("incoming_messages.json"),
    d3.json("outgoing_messages.json"),
    d3.json("incoming_messages_differences.json"),
    d3.json("outgoing_messages_differences.json")
])
    .then(function(data) {
        let incoming_downtime = data[0];
        let outgoing_downtime = data[1];
        let incoming_messages = data[2];
        let outgoing_messages = data[3];
        let incoming_messages_differences = data[4];
        let outgoing_messages_differences = data[5];

        let downtime = incoming_downtime;
        let messages = incoming_messages;
        let message_differences = incoming_messages_differences;

        // Plot Incoming Graph
        // sort data based on date objects
        downtime.sort(
            (a, b) => new Date(a.NextMessageTimeTimestamp) - new Date(b.NextMessageTimeTimestamp)
        );
        messages.sort((a, b) => new Date(a.PeriodEnd) - new Date(b.PeriodEnd));
        // Set scale domains
        x.domain(
            d3.extent(
                [].concat(
                    downtime.map(d => new Date(d.PreviousMessageTimestamp)),
                    downtime.map(d => new Date(d.NextMessageTimeTimestamp)),
                    messages.map(d => new Date(d.PeriodEnd)),
                    messages.map(d => new Date(d.PeriodStart))
                )
            )
        );
        y.domain([0, d3.max(downtime.map(d => Math.floor(d.DownTimeDurationSeconds / 60)))]);
        yRight.domain([
            d3.min(
                [].concat(
                    messages.map(d => d.NumberOfMessages),
                    message_differences.map(d => d.MessageDifference)
                )
            ),
            d3.max(
                [].concat(
                    messages.map(d => d.NumberOfMessages),
                    message_differences.map(d => d.MessageDifference)
                )
            )
        ]);
        // Update path data line 1
        path.data([message_differences])
            // .attr("fill", "yellow")
            .attr("stroke", "yellow")
            .attr("stroke-width", 1)
            .attr("d", MessageDifferenceLine);
        // Update path data line 2
        path1
            .data([messages])
            .attr("fill", "blue")
            .attr("stroke", "#00BFA5")
            .attr("stroke-width", 1)
            .attr("d", NumberOfMessagesLine);

        incomingMsgGraph
            .selectAll("path")
            .data(downtime)
            .enter()
            .append("path")
            .attr("d", makeRect)
            // .attr("stroke", "#00BFA5")
            // .attr("stroke-width", 1)
            .attr("fill", "orange")
            .style("opacity", 0)
            .transition()
            .style("opacity", 0.2)
            .duration(3500);

        // Plot Outgoing Graph
        outgoing_downtime.sort(
            (a, b) => new Date(a.NextMessageTimeTimestamp) - new Date(b.NextMessageTimeTimestamp)
        );
        outgoing_messages.sort((a, b) => new Date(a.PeriodEnd) - new Date(b.PeriodEnd));
        // Set scale domains
        outgoingMsgGraphxScale.domain(
            d3.extent(
                [].concat(
                    outgoing_downtime.map(d => new Date(d.PreviousMessageTimestamp)),
                    outgoing_downtime.map(d => new Date(d.NextMessageTimeTimestamp)),
                    outgoing_messages.map(d => new Date(d.PeriodEnd)),
                    outgoing_messages.map(d => new Date(d.PeriodStart))
                )
            )
        );
        outgoingMsgGraphyScale.domain([
            0,
            d3.max(outgoing_downtime.map(d => Math.floor(d.DownTimeDurationSeconds / 60)))
        ]);
        outgoingMsgGraphyRightScale.domain([
            d3.min(
                [].concat(
                    outgoing_messages.map(d => d.NumberOfMessages),
                    outgoing_messages_differences.map(d => d.MessageDifference)
                )
            ),
            d3.max(
                [].concat(
                    outgoing_messages.map(d => d.NumberOfMessages),
                    outgoing_messages_differences.map(d => d.MessageDifference)
                )
            )
        ]);

        // Update path data line 1
        outgoingMsgGraphPath
            .data([outgoing_messages_differences])
            // .attr("fill", "yellow")
            .attr("stroke", "yellow")
            .attr("stroke-width", 1)
            .attr("d", outgoingMsgGraphMessageDifferenceLine);

        // Update path data line 2
        outgoingMsgGraphPath1
            .data([outgoing_messages])
            .attr("fill", "blue")
            .attr("stroke", "#00BFA5")
            .attr("stroke-width", 1)
            .attr("d", outgoingMsgGraphNumberOfMessagesLine);

        outgoingMsgGraph
            .selectAll("path")
            .data(outgoing_downtime)
            .enter()
            .append("path")
            .attr("d", makeRect)
            .attr("stroke", "#00BFA5")
            .attr("stroke-width", 1)
            .attr("fill", "orange")
            .style("opacity", 0)
            .transition()
            .style("opacity", 0.2)
            .duration(3500);

        function makeRect(d, i) {
            let x0 = x(new Date(d.PreviousMessageTimestamp)),
                y0 = y(Math.floor(d.DownTimeDurationSeconds / 60)),
                x1 = x(new Date(d.NextMessageTimeTimestamp)),
                y1 = graphHeight,
                p1 = x0 + " " + y0,
                p2 = x0 + " " + y1,
                p3 = x1 + " " + y1,
                p4 = x1 + " " + y0,
                l = "L";

            return "M" + p1 + l + p2 + l + p3 + l + p4 + "Z";
        }
        // Create axes
        const xAxis = d3
            .axisBottom(x)
            .ticks(d3.timeDay.every(4))
            .tickFormat(d3.timeFormat("%Y-%m-%d"));
        // .ticks(24)
        // .tickFormat(d3.timeFormat("%b %d"));
        const outgoingMsgGraphxAxis = d3
            .axisBottom(outgoingMsgGraphxScale)
            .ticks(d3.timeDay.every(4))
            .tickFormat(d3.timeFormat("%Y-%m-%d"));
        // .ticks(24)
        // .tickFormat(d3.timeFormat("%b %d"));
        const yAxis = d3.axisLeft(y).ticks(4);
        const outgoingMsgGraphyAxis = d3.axisLeft(outgoingMsgGraphyScale).ticks(4);
        const yAxis2 = d3.axisRight(yRight).ticks(4);
        const outgoingMsgGraphyAxis2 = d3.axisRight(outgoingMsgGraphyRightScale).ticks(4);
        // .tickFormat(d => (d = "m"));

        // Call axes
        incomingMsgGraphxAxisGroup.call(xAxis);
        outgoingMsgGraphxAxisGroup.call(outgoingMsgGraphxAxis);
        incomingMsgGraphyAxisGroup.call(yAxis);
        outgoingMsgGraphLeftYAxisGroup.call(outgoingMsgGraphyAxis2);
        yAxisGroup2.call(yAxis2);
        outgoingMsgGraphRightYAxisGroup.call(outgoingMsgGraphyAxis);

        // Rotae axis text
        incomingMsgGraphxAxisGroup
            .selectAll("text")
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(-65)");
        outgoingMsgGraphxAxisGroup
            .selectAll("text")
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(-65)");
    })
    .catch(function(err) {
        // handle error here
        console.log(err);
    });
