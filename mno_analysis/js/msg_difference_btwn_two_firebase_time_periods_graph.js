
export class MessageDifference {
    static drawGraph(data_path) {
        let margin = { top: 60, right: 100, bottom: 120, left: 70 },
            width = 960 - margin.left - margin.right,
            height = 500 - margin.top - margin.bottom;

        // Append the svg object to the body of the page
        let msgDifferenceGraph = d3.select("#line-graph")
            .append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        // Graph title
        msgDifferenceGraph
            .append("text")
            .attr("x", width / 2)
            .attr("y", 0 - margin.top / 2)
            .attr("text-anchor", "middle")
            .style("font-size", "20px")
            .style("text-decoration", "bold")
            .text("Message Difference between Two Firebase Time Periods (rN = d(n)/d(t))");

        msgDifferenceGraph
            .append("text")
            .attr("x", width / 2)
            .attr("y", (0 - margin.top / 2) + 20)
            .attr("text-anchor", "middle")
            .style("font-size", "20px")
            .style("text-decoration", "bold")
            .text("(The time period for firebase is 10 min)");

        // X axis Label 
        msgDifferenceGraph
            .append("text")
            .attr(
                "transform",
                "translate(" + width / 2 + " ," + (height + margin.top + 50) + ")"
            )
            .style("text-anchor", "middle")
            .text("Date (Y-M-D) when two firebase time periods ended");

        // Y axis Label
        msgDifferenceGraph
            .append("text")
            .attr("transform", "rotate(-90)")
            .attr("y", 0 - margin.left)
            .attr("x", 0 - height / 2)
            .attr("dy", "1em")
            .style("text-anchor", "middle")
            .text("No. of Message (s)");

        d3.json(data_path).then(data => {
            data.forEach(d => {
                d.date = new Date(d.PeriodEnd);
                d.value = +d.MessageDifference;
            });
            // Create a date formatter to display a shorter date
            const formatDate = d3.timeFormat("%Y-%m-%d");
        
            // Add X axis for main chart --> it is a date format
            let x = d3.scaleTime()
                .domain(d3.extent(data, d => d.date))
                .range([0, width]);
            let xAxis = msgDifferenceGraph
                .append("g")
                .attr("transform", "translate(0," + height + ")")
                .call(d3.axisBottom(x).tickFormat(formatDate))
            xAxis
                .selectAll("text")
                .attr("transform", "translate(-10,0)rotate(-45)")
                .style("text-anchor", "end");
        
            // Add Y axis
            let y = d3.scaleLinear()
                .domain(d3.extent(data, d => +d.value))
                .range([height, 0]);
            msgDifferenceGraph.append("g").call(d3.axisLeft(y).ticks(5));
        
            // Add a clipPath: everything out of this area won't be drawn.
            let clip = msgDifferenceGraph
                .append("defs")
                .append("svg:clipPath")
                .attr("id", "clip")
                .append("svg:rect")
                .attr("width", width)
                .attr("height", height)
                .attr("x", 0)
                .attr("y", 0);
        
            // Create the area variable: where both the area and the brush take place
            let line = msgDifferenceGraph.append("g").attr("clip-path", "url(#clip)");
            let lineGenerator = d3.line().x(d => x(d.date)).y((d) => y(d.value));

            // Add brushing
            let brush = d3.brushX()  // Add the brush feature using the d3.brush function                 
                .extent( [ [0,0], [width,height] ] )  // initialise the brush area: start at 0,0 and finishes at width,height: it means I select the whole graph area
                .on("end", updateChart) // Each time the brush selection changes, trigger the 'updateChart' function

            // Add the brushing
            line.append("g")
                .attr("class", "brush")
                .call(brush);

            // A function that set idleTimeOut to null
            let idleTimeout
            function idled() { idleTimeout = null; }

            // A function that update the chart for given boundaries
            function updateChart() {

                // What are the selected boundaries?
                let extent = d3.event.selection

                // If no selection, back to initial coordinate. Otherwise, update X axis domain
                if (!extent) {
                    if (!idleTimeout) return idleTimeout = setTimeout(idled, 350); // This allows to wait a little bit
                    x.domain(d3.extent(data, d => d.date))
                } else {
                    x.domain([ x.invert(extent[0]), x.invert(extent[1]) ])
                    line.select(".brush").call(brush.move, null) // This remove the grey brush area as soon as the selection has been done
                }

                // Update axis and line position
                xAxis.transition().duration(1000).call(d3.axisBottom(x).tickFormat(formatDate))
                xAxis
                    .selectAll("text")
                    .attr("transform", "translate(-10,0)rotate(-45)")
                    .style("text-anchor", "end");
                line
                    .select('.line-graph')
                    .transition()
                    .duration(1000)
                    .attr("d", lineGenerator)
                    
            }
        
            // Create focus object for tooltip and circle
            function drawFocus() {
                // Create focus object
                let focus = msgDifferenceGraph.append("g").attr("class", "focus");
                // Add a y-line to show where hovering
                focus.append("line").classed("y", true);
                // Append circle on the line path
                focus.append("circle").attr("r", 4.5);

                // Add background rectangle behind the text tooltip
                focus.append("rect")
                    .attr("x", -30)
                    .attr("y", "-2em")
                    .attr("width", 70)
                    .attr("height", 20)
                    .style("fill", "white");
        
                // Add text annotation for tooltip
                focus.append("text")
                    .attr("x", -30)
                    .attr("dy", "-1em")
                    .attr("font-size", "12px")
                    .style("fill", "black");
        
                focus.append("div")
                    .attr("x", 10)
                    .attr("dy", ".35em")
                    .attr("class", "tooltip")
                    .style("opacity", 1);
        
                line.attr("width", width)
                    .attr("height", height)
                    .on("mouseover", () => focus.style("display", null))
                    .on("mouseout", () => focus.style("display", "none"))
                    .on("mousemove", tipMove);
        
                // Make the overlay rectangle transparent,
                // so it only serves the purpose of detecting mouse events
                d3.select(".overlay").style("fill", "none").style("pointer-events", "all");
        
                // Select focus objects and set opacity
                d3.selectAll(".focus").style("opacity", 0.9);
        
                // Select the circle and style it
                d3.selectAll(".focus circle").style("fill", "blue").style("opacity", 0);
        
                // Select the hover lines and style them
                d3.selectAll(".focus line")
                    .style("fill", "none")
                    .style("stroke", "black")
                    .style("opacity", 0.4)
                    .style("stroke-width", "1px");
        
                // Function that adds tooltip on hover
                function tipMove() {
                    // This will select the closest date on the x axis when a user hover over the chart
                    let bisectDate = d3.bisector((d) => d.date).left;
                    // Below code finds the date by bisecting and
                    // stores the x and y coordinate as variables
                    let x0 = x.invert(d3.mouse(this)[0]);
                    let i = bisectDate(data, x0, 1);
                    let d0 = data[i - 1];
                    let d1 = data[i];
                    let d = x0 - d0.date > d1.date - x0 ? d1 : d0;
        
                    // Place the focus objects on the same path as the line
                    focus.attr("transform", `translate(${x(d.date)}, ${y(d.value)})`);
        
                    // Position the x line
                    focus.select("line.x").attr("x1", 0).attr("x2", x(d.date)).attr("y1", 0).attr("y2", 0);
        
                    // Position the y line
                    focus.select("line.y")
                        .attr("x1", 0)
                        .attr("x2", 0)
                        .attr("y1", 0)
                        .attr("y2", height - y(d.value));
        
                    // Position the text
                    focus.select("text")
                        .text(`${d3.timeFormat("%Y-%m-%d (%H:%M)")(d.date)} Value: ${d.value}`)
                        .transition() // slowly fade in the tooltip
                        .duration(100)
                        .style("opacity", 1);
        
                    // Show the circle on the path
                    focus.selectAll(".focus circle").style("opacity", 1);
                }
            }
            drawFocus();
        
            line.append("path")
                .datum(data)
                .attr("class", "line-graph")
                .attr("fill", "none")
                .attr("stroke", "blue")
                .attr("stroke-width", 2)
                .attr("d", lineGenerator);

        }, err => {
            console.log(err)
        });    
    }
}
