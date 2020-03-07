// Set the dimensions and margins of the graphs
const margin = { top: 50, right: 30, bottom: 70, left: 70 },
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
    incomingMsgGraphxScale = d3.scaleTime().range([0, graphWidth]),
    incomingMsgGraphLeftyScale = d3.scaleLinear().range([graphHeight, 0]),
    incomingMsgGraphyRightScale = d3.scaleLinear().range([graphHeight, 0]),
    // Axes group
    incomingMsgGraphxAxisGroup = incomingMsgGraph
        .append("g")
        .attr("class", "x-axis")
        .attr("transform", "translate(0," + graphHeight + ")"),
    incomingMsgGraphLeftyAxisGroup = incomingMsgGraph.append("g").attr("class", "y-axis"),
    incomingMsgGraphRightYAxisGroup = incomingMsgGraph
        .append("g")
        .attr("transform", "translate( " + graphWidth + ", 0 )")
        .attr("class", "y-axis2"),
    // Line
    MessageDifferenceLine = d3
        .line()
        .x(d => incomingMsgGraphxScale(new Date(d.PeriodBetween)))
        .y(d => incomingMsgGraphyRightScale(d.MessageDifference)),
    NumberOfMessagesLine = d3
        .line()
        .x(d => incomingMsgGraphxScale(new Date(d.PeriodEnd)))
        .y(d => incomingMsgGraphyRightScale(d.NumberOfMessages)),
    // d3 line path generator
    incomingMsgGraphNumberOfMessagesLinePath = incomingMsgGraph.append("path"),
    incomingMsgGraphMessageDifferenceLinePath = incomingMsgGraph.append("path");

// Total Incoming Sms(s) graph title
incomingMsgGraph
    .append("text")
    .attr("x", graphWidth / 2)
    .attr("y", 0 - margin.top / 1.4)
    .attr("text-anchor", "middle")
    .style("font-size", "20px")
    .style("text-decoration", "bold")
    .text("Total Incoming Messages");

// Y axis Label for the total incoming sms graph
incomingMsgGraph
    .append("text")
    .attr("transform", "rotate(-90)")
    .attr("y", 0 - margin.left)
    .attr("x", 0 - graphHeight / 2)
    .attr("dy", "1em")
    .style("text-anchor", "middle")
    .text("Downtime Duration in Seconds");

// Handmade legend
incomingMsgGraphSvg
    .append("circle")
    .attr("cx", 100)
    .attr("cy", 27)
    .attr("r", 6)
    .style("fill", "yellow");
incomingMsgGraphSvg
    .append("text")
    .attr("x", 120)
    .attr("y", 30)
    .text("message difference between two firebase time periods")
    .style("font-size", "12px")
    .attr("alignment-baseline", "middle");
incomingMsgGraphSvg
    .append("circle")
    .attr("cx", 450)
    .attr("cy", 27)
    .attr("r", 6)
    .style("fill", "#00BFA5");
incomingMsgGraphSvg
    .append("text")
    .attr("x", 470)
    .attr("y", 30)
    .text("Messages received per time")
    .style("font-size", "12px")
    .attr("alignment-baseline", "middle");
incomingMsgGraphSvg
    .append("circle")
    .attr("cx", 720)
    .attr("cy", 27)
    .attr("r", 6)
    .style("fill", "orange");
incomingMsgGraphSvg
    .append("text")
    .attr("x", 740)
    .attr("y", 30)
    .text("window of time with 0 messages")
    .style("font-size", "12px")
    .attr("alignment-baseline", "middle");

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
    outgoingMsgGraphLeftyScale = d3.scaleLinear().range([graphHeight, 0]),
    outgoingMsgGraphRightyScale = d3.scaleLinear().range([graphHeight, 0]),
    // Axes groups
    outgoingMsgGraphxAxisGroup = outgoingMsgGraph
        .append("g")
        .attr("class", "x-axis")
        .attr("transform", "translate(0," + graphHeight + ")"),
    outgoingMsgGraphLeftYAxisGroup = outgoingMsgGraph.append("g").attr("class", "left-y-axis"),
    outgoingMsgGraphRightYAxisGroup = outgoingMsgGraph
        .append("g")
        .attr("transform", "translate( " + graphWidth + ", 0 )")
        .attr("class", "right-y-axis"),
    // Line
    outgoingMsgGraphMessageDifferenceLine = d3
        .line()
        .x(d => outgoingMsgGraphxScale(new Date(d.PeriodBetween)))
        .y(d => outgoingMsgGraphRightyScale(d.MessageDifference)),
    outgoingMsgGraphNumberOfMessagesLine = d3
        .line()
        .x(d => outgoingMsgGraphxScale(new Date(d.PeriodEnd)))
        .y(d => outgoingMsgGraphRightyScale(d.NumberOfMessages)),
    // d3 line path generator
    outgoingMsgGraphNumberOfMessagesLinePath = outgoingMsgGraph.append("path"),
    outgoingMsgGraphMessageDifferenceLinePath = outgoingMsgGraph.append("path");

// Total Outgoing Sms(s) graph title
outgoingMsgGraph
    .append("text")
    .attr("x", graphWidth / 2)
    .attr("y", 0 - margin.top / 1.4)
    .attr("text-anchor", "middle")
    .style("font-size", "20px")
    .style("text-decoration", "bold")
    .text("Total Outgoing Messages");

// Y axis Label for the total outgoing sms graph
outgoingMsgGraph
    .append("text")
    .attr("transform", "rotate(-90)")
    .attr("y", 0 - margin.left)
    .attr("x", 0 - graphHeight / 2)
    .attr("dy", "1em")
    .style("text-anchor", "middle")
    .text("Downtime Duration in Seconds");
// Handmade legend
outgoingMsgGraphSvg
    .append("circle")
    .attr("cx", 100)
    .attr("cy", 27)
    .attr("r", 6)
    .style("fill", "yellow");
outgoingMsgGraphSvg
    .append("text")
    .attr("x", 120)
    .attr("y", 30)
    .text("message difference between two firebase time periods")
    .style("font-size", "12px")
    .attr("alignment-baseline", "middle");
outgoingMsgGraphSvg
    .append("circle")
    .attr("cx", 450)
    .attr("cy", 27)
    .attr("r", 6)
    .style("fill", "#00BFA5");
outgoingMsgGraphSvg
    .append("text")
    .attr("x", 470)
    .attr("y", 30)
    .text("Messages received per time")
    .style("font-size", "12px")
    .attr("alignment-baseline", "middle");
outgoingMsgGraphSvg
    .append("circle")
    .attr("cx", 720)
    .attr("cy", 27)
    .attr("r", 6)
    .style("fill", "orange");
outgoingMsgGraphSvg
    .append("text")
    .attr("x", 740)
    .attr("y", 30)
    .text("window of time with 0 messages")
    .style("font-size", "12px")
    .attr("alignment-baseline", "middle");

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

        const makeRect = d => {
            let x0 = incomingMsgGraphxScale(new Date(d.PreviousMessageTimestamp)),
                y0 = incomingMsgGraphLeftyScale(Math.floor(d.DownTimeDurationSeconds / 3600)),
                x1 = incomingMsgGraphxScale(new Date(d.NextMessageTimestamp)),
                y1 = graphHeight,
                p1 = x0 + " " + y0,
                p2 = x0 + " " + y1,
                p3 = x1 + " " + y1,
                p4 = x1 + " " + y0,
                l = "L";

            return "M" + p1 + l + p2 + l + p3 + l + p4 + "Z";
        };

        // Plot Incoming Graph
        // sort data based on date objects
        incoming_downtime.sort(
            (a, b) => new Date(a.NextMessageTimestamp) - new Date(b.NextMessageTimestamp)
        );
        incoming_messages.sort((a, b) => new Date(a.PeriodEnd) - new Date(b.PeriodEnd));
        // Set scale domains
        incomingMsgGraphxScale.domain(
            d3.extent(
                [].concat(
                    incoming_downtime.map(d => new Date(d.PreviousMessageTimestamp)),
                    incoming_downtime.map(d => new Date(d.NextMessageTimestamp)),
                    incoming_messages.map(d => new Date(d.PeriodEnd)),
                    incoming_messages.map(d => new Date(d.PeriodStart))
                )
            )
        );
        incomingMsgGraphLeftyScale.domain([
            0,
            d3.max(incoming_downtime.map(d => Math.floor(d.DownTimeDurationSeconds / 3600)))
        ]);
        incomingMsgGraphyRightScale.domain([
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
        incomingMsgGraphMessageDifferenceLinePath
            .data([incoming_messages_differences])
            .attr("stroke", "yellow")
            .attr("stroke-width", 1)
            .attr("d", MessageDifferenceLine);
        // Update path data line 2
        incomingMsgGraphNumberOfMessagesLinePath
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
            .attr("fill", "orange")
            .style("opacity", 0)
            .transition()
            .style("opacity", 0.2)
            .duration(3500);
        // Create axes
        const incomingMsgGraphxAxis = d3
            .axisBottom(incomingMsgGraphxScale)
            .ticks(d3.timeDay.every(4))
            .tickFormat(d3.timeFormat("%Y-%m-%d"))
            .ticks(24);

        const incomingMsgGraphLeftyAxis = d3.axisLeft(incomingMsgGraphLeftyScale).ticks(10);
        const incomingMsgGraphRightyAxis = d3.axisRight(incomingMsgGraphyRightScale).ticks(10);
        // Call axes
        incomingMsgGraphxAxisGroup.call(incomingMsgGraphxAxis);
        incomingMsgGraphLeftyAxisGroup.call(incomingMsgGraphLeftyAxis);
        incomingMsgGraphRightYAxisGroup.call(incomingMsgGraphRightyAxis);
        // Rotate axis text
        incomingMsgGraphxAxisGroup
            .selectAll("text")
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(-65)");

        // Plot Outgoing Graph
        outgoing_downtime.sort(
            (a, b) => new Date(a.NextMessageTimestamp) - new Date(b.NextMessageTimestamp)
        );
        outgoing_messages.sort((a, b) => new Date(a.PeriodEnd) - new Date(b.PeriodEnd));
        // Set scale domains
        outgoingMsgGraphxScale.domain(
            d3.extent(
                [].concat(
                    outgoing_downtime.map(d => new Date(d.PreviousMessageTimestamp)),
                    outgoing_downtime.map(d => new Date(d.NextMessageTimestamp)),
                    outgoing_messages.map(d => new Date(d.PeriodEnd)),
                    outgoing_messages.map(d => new Date(d.PeriodStart))
                )
            )
        );
        outgoingMsgGraphLeftyScale.domain([
            0,
            d3.max(outgoing_downtime.map(d => Math.floor(d.DownTimeDurationSeconds / 3600)))
        ]);
        outgoingMsgGraphRightyScale.domain([
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
        outgoingMsgGraphMessageDifferenceLinePath
            .data([outgoing_messages_differences])
            .attr("stroke", "yellow")
            .attr("stroke-width", 1)
            .attr("d", outgoingMsgGraphMessageDifferenceLine);

        // Update path data line 2
        outgoingMsgGraphNumberOfMessagesLinePath
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
            .attr("fill", "orange")
            .style("opacity", 0)
            .transition()
            .style("opacity", 0.2)
            .duration(3500);

        // Create axes
        const outgoingMsgGraphxAxis = d3
            .axisBottom(outgoingMsgGraphxScale)
            .ticks(d3.timeDay.every(4))
            .tickFormat(d3.timeFormat("%Y-%m-%d"))
            .ticks(24);
        const outgoingMsgGraphLeftyAxis = d3.axisLeft(outgoingMsgGraphLeftyScale).ticks(10);
        const outgoingMsgGraphRightyAxis = d3.axisRight(outgoingMsgGraphRightyScale).ticks(10);

        // Call axes
        outgoingMsgGraphxAxisGroup.call(outgoingMsgGraphxAxis);
        outgoingMsgGraphLeftYAxisGroup.call(outgoingMsgGraphLeftyAxis);
        outgoingMsgGraphRightYAxisGroup.call(outgoingMsgGraphRightyAxis);

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
