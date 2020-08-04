// Set the dimensions and margins of the graph
let margin = { top: 40, right: 100, bottom: 105, left: 70 },
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

// X axis Label 
msgDifferenceGraph
    .append("text")
    .attr(
        "transform",
        "translate(" + width / 2 + " ," + (height + margin.top + 50) + ")"
    )
    .style("text-anchor", "middle")
    .text("Date (Y-M-D)");

// Y axis Label
msgDifferenceGraph
    .append("text")
    .attr("transform", "rotate(-90)")
    .attr("y", 0 - margin.left)
    .attr("x", 0 - height / 2)
    .attr("dy", "1em")
    .style("text-anchor", "middle")
    .text("No. of Message (s)");

// this will select the closest date on the x axiswhen a user hover over the chart
let bisectDate = d3.bisector((d) => d.date).left;
// create number formatter to 2 decmials for tooltip
const formatDecimal = d3.format(".2f");
// create a date formatter to display a shorter date
const formatDate = d3.timeFormat("%Y-%m-%d");
// set the width of the variable
const tooltipWidth = 300;

let data_path = `./data/incoming_messages/incoming_msg_diff_per_period.json`;
// Update chart data
d3.select("#in").on("click", () => {
    data_path = `./data/incoming_messages/incoming_msg_diff_per_period.json`;
    drawGraph(data_path)
});
d3.select("#out").on("click", () => {
    data_path = `./data/outgoing_messages/outgoing_msg_diff_per_period.json`;
    drawGraph(data_path)
});

drawGraph(data_path)

function drawGraph(data_path) {
    console.log(data_path)
    d3.json(data_path).then(function (data) {
        data.forEach(function (d) {
            d.date = new Date(d.PeriodEnd);
            d.value = +d.MessageDifference;
        });
    
        // Add X axis for main chart --> it is a date format
        let x = d3.scaleTime()
            .domain(d3.extent(data, d => d.date))
            .range([0, width]);
        msgDifferenceGraph
            .append("g")
            .attr("transform", "translate(0," + height + ")")
            .call(d3.axisBottom(x).tickFormat(formatDate))
            .selectAll("text")
            .attr("transform", "translate(-10,0)rotate(-45)")
            .style("text-anchor", "end");
    
        // Add Y axis
        let y = d3.scaleLinear()
            .domain([0, d3.max(data, d => +d.value)])
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
        let lineGenerator = d3.line().x((d) => x(d.date)).y((d) => y(d.value));
    
        // Create focus object for tooltip and circle
        function drawFocus() {
            // Create focus object
            let focus = msgDifferenceGraph.append("g").attr("class", "focus");
            // Add a y-line to show where hovering
            focus.append("line").classed("y", true);
            // Append circle on the line path
            focus.append("circle").attr("r", 7.5);

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
                .style("fill", "black")
                .style("font-family", "SuisseIntl");
    
            focus.append("div")
                .attr("x", 10)
                .attr("dy", ".35em")
                .attr("class", "tooltip")
                .style("opacity", 1);
    
            // Create an overlay rectangle to draw the above objects on top of
            msgDifferenceGraph.append("rect")
                .attr("class", "overlay")
                .attr("width", width)
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
            d3.selectAll(".focus circle").style("fill", "#068ca0").style("opacity", 0);
    
            // Select the hover lines and style them
            d3.selectAll(".focus line")
                .style("fill", "none")
                .style("stroke", "black")
                .style("opacity", 0.4)
                .style("stroke-width", "1px");
    
            // Function that adds tooltip on hover
            function tipMove() {
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
                    .text(d.value)
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
            .attr("fill", "none")
            .attr("stroke", "blue")
            .attr("stroke-width", 2)
            .attr("d", lineGenerator);
            
    }, err => {
        console.log(err)
    });    
}
