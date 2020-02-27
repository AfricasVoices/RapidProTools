// Set the dimensions and margins of the graph
const margin = { top: 20, right: 30, bottom: 70, left: 50 },
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
        .x(d => x(new Date(d.NextMessageTimeTimestamp)))
        .y(d => y(d.DownTimeDurationSeconds)),
    line2 = d3
        .line()
        .x(d => x(new Date(d.periodEnd)))
        .y(d => yRight(d.NumberOfMessages)),
    dotLines = graph.append("g").attr("class", "lines"),
    xdotlines = dotLines
        .append("line")
        .attr("stroke", "black")
        .attr("stroke-width", 1)
        .attr("stroke-dasharray", 4),
    xdotlines2 = dotLines
        .append("line")
        .attr("stroke", "black")
        .attr("stroke-width", 1)
        .attr("stroke-dasharray", 4),
    ydotlines = dotLines
        .append("line")
        .attr("stroke", "black")
        .attr("stroke-width", 1)
        .attr("stroke-dasharray", 4),
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
                    data[0].map(d => new Date(d.PreviousMessageTimestamp)),
                    data[0].map(d => new Date(d.NextMessageTimeTimestamp)),
                    data[1].map(d => new Date(d.periodEnd)),
                    data[1].map(d => new Date(d.periodStart))
                )
            )
        );
        y.domain([0, d3.max(data[0].map(d => Math.floor(d.DownTimeDurationSeconds % 60)))]);
        yRight.domain([0, d3.max(data[1].map(d => d.NumberOfMessages))]);

        // Update path data line 1
        // path.data([data[0]])
        //     .attr("fill", "none")
        //     .attr("stroke", "#00BFA5")
        //     .attr("stroke-width", 2)
        //     .attr("d", line);

        // Update path data line 2
        path1
            .data([data[1]])
            .attr("fill", "none")
            .attr("stroke", "#00BFA5")
            .attr("stroke-width", 2)
            .attr("d", line2);

        const circles = graph.selectAll("circle").data(data[0]);
        const circles1 = graph.selectAll("circle1").data(data[0]);
        const circles2 = graph.selectAll("circle2").data(data[1]);

        graph
            .selectAll("path")
            .data(data[0])
            .enter()
            .append("path")
            .attr("d", makeRect)
            // .attr("stroke", "#00BFA5")
            // .attr("stroke-width", 1)
            .attr("fill", "grey")
            .style("opacity", 0)
            .transition()
            .style("opacity", 1)
            .duration(3500);

        function makeRect(d, i) {
            let x0 = x(new Date(d.PreviousMessageTimestamp)),
                y0 = y(Math.floor(d.DownTimeDurationSeconds % 60)),
                x1 = x(new Date(d.NextMessageTimeTimestamp)),
                y1 = graphHeight,
                p1 = x0 + " " + y0,
                p2 = x0 + " " + y1,
                p3 = x1 + " " + y1,
                p4 = x1 + " " + y0,
                l = "L";

            return "M" + p1 + l + p2 + l + p3 + l + p4 + "Z";
        }

        // Add new points
        circles
            .enter()
            .append("circle")
            .attr("r", 2)
            .attr("cx", d => x(new Date(d.NextMessageTimeTimestamp)))
            .attr("cy", d => y(d.DownTimeDurationSeconds))
            .attr("fill", "red");

        circles1
            .enter()
            .append("circle")
            .attr("r", 2)
            .attr("cx", d => x(new Date(d.PreviousMessageTimestamp)))
            .attr("cy", d => y(d.DownTimeDurationSeconds))
            .attr("fill", "white");

        // Add new points
        circles2
            .enter()
            .append("circle")
            .attr("r", 1)
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
            .ticks(d3.timeDay.every(4))
            .tickFormat(d3.timeFormat("%Y-%m-%d"));
        // .ticks(24)
        // .tickFormat(d3.timeFormat("%b %d"));
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
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(-65)");
    })
    .catch(function(err) {
        // handle error here
        console.log(err);
    });
