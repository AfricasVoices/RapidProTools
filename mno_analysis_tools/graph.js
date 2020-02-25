// Set the dimensions and margins of the graph
const margin = { top: 20, right: 30, bottom: 30, left: 50 },
    graphWidth = 960 - margin.left - margin.right,
    graphHeight = 500 - margin.top - margin.bottom,
    // svg
    svg = d3
        .select("body")
        .append("svg")
        .attr("width", graphWidth + margin.left + margin.right)
        .attr("height", graphHeight + margin.top + margin.bottom),
    // graph
    graph = svg
        .append("g")
        .attr("width", graphWidth)
        .attr("Height", graphHeight)
        .attr("transform", `translate(${margin.left}, ${margin.top})`),
    // Scales
    x = d3.scaleTime().range([0, graphWidth]),
    y = d3.scaleLinear().range([graphHeight, 0]),
    yRight = d3.scaleLinear().range([graphHeight, 0]),
    // Axes groups
    xAxisGroup = graph
        .append("g")
        .attr("class", "x-axis")
        .attr("transform", "translate(0," + graphHeight + ")"),
    yAxisGroup = graph.append("g").attr("class", "y-axis"),
    yAxisGroup2 = graph
        .append("g")
        .attr("transform", "translate( " + graphWidth + ", 0 )")
        .attr("class", "y-axis2"),
    line = d3
        .line()
        .x(function(d) {
            return x(new Date(d.NextMessageTimeTimestamp));
        })
        .y(function(d) {
            return y(d.DownTimeDurationSeconds);
        }),
    line2 = d3
        .line()
        .x(function(d) {
            return x(new Date(d.periodEnd));
        })
        .y(function(d) {
            return yRight(d.NumberOfMessages);
        }),
    // d3 line path generator
    path = graph.append("path"),
    path1 = graph.append("path");

Promise.all([d3.json("down.json"), d3.json("msgs.json")])
    .then(function(data) {
        // files[0] will contain file1.json
        // files[1] will contain file2.json
        // sort data based on date objects
        data[0].sort(
            (a, b) => new Date(a.NextMessageTimeTimestamp) - new Date(b.NextMessageTimeTimestamp)
        );
        data[1].sort((a, b) => new Date(a.periodEnd) - new Date(b.periodEnd));
        // Set scale domains
        x.domain(
            d3.extent(
                [].concat(
                    data[0].map(d => new Date(d.NextMessageTimeTimestamp)),
                    data[1].map(d => new Date(d.periodEnd))
                )
            )
        );
        y.domain([0, d3.max(data[0].map(d => d.DownTimeDurationSeconds))]);
        yRight.domain([0, d3.max(data[1].map(d => d.NumberOfMessages))]);

        // console.log(data[0].map(item => new Date(item.NextMessageTimeTimestamp)));
        // console.log(data[1].map(item => new Date(item.periodEnd)));
        // Update path data line 1
        path.data([data[0]])
            .attr("fill", "none")
            .attr("stroke", "#00BFA5")
            .attr("stroke-width", 2)
            .attr("d", line);

        // Update path data line 2
        path1
            .data([data[1]])
            .attr("fill", "none")
            .attr("stroke", "#00BFA5")
            .attr("stroke-width", 2)
            .attr("d", line2);

        const circles = graph.selectAll("circle").data(data[0]);
        const circles2 = graph.selectAll("circle2").data(data[1]);

        // Remove unwanted points
        // circles.exit().remove()
        // Update current points
        // circles
        //     .attr("cx", d => x(new Date(d.date)))
        //     .attr("cy", d => y(d.distance))

        // Add new points
        circles
            .enter()
            .append("circle")
            .attr("r", 4)
            .attr("cx", d => x(new Date(d.NextMessageTimeTimestamp)))
            .attr("cy", d => y(d.DownTimeDurationSeconds))
            .attr("fill", "#CCC");

        // Add new points
        circles2
            .enter()
            .append("circle")
            .attr("r", 4)
            .attr("cx", d => x(new Date(d.periodEnd)))
            .attr("cy", d => yRight(d.NumberOfMessages))
            .attr("fill", "#CCC");

        graph
            .selectAll("circle")
            .on("mouseover", (d, i, n) => {
                d3.select(n[i])
                    .transition()
                    .duration(100)
                    .attr("r", 8)
                    .attr("fill", "#FFF");
            })
            .on("mouseleave", (d, i, n) => {
                d3.select(n[i])
                    .transition()
                    .duration(100)
                    .attr("r", 4)
                    .attr("fill", "#CCC");
            });
        // Create axes
        const xAxis = d3
            .axisBottom(x)
            .ticks(4)
            .tickFormat(d3.timeFormat("%b %d"));
        const yAxis = d3.axisLeft(y).ticks(4);
        const yAxis2 = d3.axisRight(yRight).ticks(4);
        // .tickFormat(d => (d = "m"));

        // Call axes
        xAxisGroup.call(xAxis);
        yAxisGroup.call(yAxis);
        yAxisGroup2.call(yAxis2);

        // Rotae axis text
        xAxisGroup
            .selectAll("text")
            .attr("transform", "rotate(-40)")
            .attr("text-anchor", end);
    })
    .catch(function(err) {
        // handle error here
    });
