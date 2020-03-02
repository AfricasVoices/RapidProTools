// Set the dimensions and margins of the graph
const margin = { top: 20, right: 30, bottom: 70, left: 50 },
    graphWidth = 960 - margin.left - margin.right,
    graphHeight = 500 - margin.top - margin.bottom;
// --------------------------------------------------------------------------------------------------

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
// --------------------------------------------------------------------------------------------------

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
// --------------------------------------------------------------------------------------------------

Promise.all([
    d3.json("incoming_msg.json"),
    d3.json("outgoing_msg.json"),
    d3.json("incoming_msg_downtime.json"),
    d3.json("outgoing_msg_downtime.json"),
    d3.json("incoming_msg_diff_per_period.json"),
    d3.json("outgoing_msg_diff_per_period.json")
])
    .then(function(data) {
        let incoming_messages = data[0],
            outgoing_messages = data[1],
            incoming_downtime = data[2],
            outgoing_downtime = data[3],
            incoming_messages_differences = data[4],
            outgoing_messages_differences = data[5];

        const makeRect = (d, i) => {
            let x0 = x(new Date(d.PreviousMessageTimestamp)),
                y0 = y(Math.floor(d.DownTimeDurationSeconds / 3600)),
                x1 = x(new Date(d.NextMessageTimeTimestamp)),
                y1 = graphHeight,
                p1 = x0 + " " + y0,
                p2 = x0 + " " + y1,
                p3 = x1 + " " + y1,
                p4 = x1 + " " + y0,
                l = "L";

            return "M" + p1 + l + p2 + l + p3 + l + p4 + "Z";
        };
        // --------------------------------------------------------------------------------------------------

        // Plot Incoming Graph
        // sort data based on date objects
        incoming_downtime.sort(
            (a, b) => new Date(a.NextMessageTimeTimestamp) - new Date(b.NextMessageTimeTimestamp)
        );
        incoming_messages.sort((a, b) => new Date(a.PeriodEnd) - new Date(b.PeriodEnd));
        // Set scale domains
        x.domain(
            d3.extent(
                [].concat(
                    incoming_downtime.map(d => new Date(d.PreviousMessageTimestamp)),
                    incoming_downtime.map(d => new Date(d.NextMessageTimeTimestamp)),
                    incoming_messages.map(d => new Date(d.PeriodEnd)),
                    incoming_messages.map(d => new Date(d.PeriodStart))
                )
            )
        );
        y.domain([
            0,
            d3.max(incoming_downtime.map(d => Math.floor(d.DownTimeDurationSeconds / 3600)))
        ]);
        yRight.domain([
            d3.min(
                [].concat(
                    incoming_messages.map(d => d.NumberOfMessages),
                    incoming_messages_differences.map(d => d.MessageDifference)
                )
            ),
            d3.max(
                [].concat(
                    incoming_messages.map(d => d.NumberOfMessages),
                    incoming_messages_differences.map(d => d.MessageDifference)
                )
            )
        ]);
        // Update path data line 1
        path.data([incoming_messages_differences])
            // .attr("fill", "yellow")
            .attr("stroke", "yellow")
            .attr("stroke-width", 1)
            .attr("d", MessageDifferenceLine);
        // Update path data line 2
        path1
            .data([incoming_messages])
            .attr("fill", "blue")
            .attr("stroke", "#00BFA5")
            .attr("stroke-width", 1)
            .attr("d", NumberOfMessagesLine);

        incomingMsgGraph
            .selectAll("path")
            .data(incoming_downtime)
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
        // Create axes
        const xAxis = d3
            .axisBottom(x)
            .ticks(d3.timeDay.every(4))
            .tickFormat(d3.timeFormat("%Y-%m-%d"));
        // .ticks(24)
        // .tickFormat(d3.timeFormat("%b %d"));

        const yAxis = d3.axisLeft(y).ticks(4);
        const yAxis2 = d3.axisRight(yRight).ticks(4);
        // Call axes
        incomingMsgGraphxAxisGroup.call(xAxis);
        incomingMsgGraphyAxisGroup.call(yAxis);
        yAxisGroup2.call(yAxis2);
        // Rotate axis text
        incomingMsgGraphxAxisGroup
            .selectAll("text")
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(-65)");
        // --------------------------------------------------------------------------------------------------

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
            d3.max(outgoing_downtime.map(d => Math.floor(d.DownTimeDurationSeconds / 3600)))
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

        // Create axes
        const outgoingMsgGraphxAxis = d3
            .axisBottom(outgoingMsgGraphxScale)
            .ticks(d3.timeDay.every(4))
            .tickFormat(d3.timeFormat("%Y-%m-%d"));
        // .ticks(24)
        // .tickFormat(d3.timeFormat("%b %d"));
        const outgoingMsgGraphyAxis = d3.axisLeft(outgoingMsgGraphyScale).ticks(4);
        const outgoingMsgGraphyAxis2 = d3.axisRight(outgoingMsgGraphyRightScale).ticks(4);
        // .tickFormat(d => (d = "m"));

        // Call axes
        outgoingMsgGraphxAxisGroup.call(outgoingMsgGraphxAxis);
        outgoingMsgGraphLeftYAxisGroup.call(outgoingMsgGraphyAxis2);
        outgoingMsgGraphRightYAxisGroup.call(outgoingMsgGraphyAxis);

        // Rotate axis text
        outgoingMsgGraphxAxisGroup
            .selectAll("text")
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(-65)");
    })
    .catch(function(err) {
        alert(err);
    });
