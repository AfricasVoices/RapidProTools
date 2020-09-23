export class WindowOfDowntime {
    static drawGraph(data_path, threshold) {
        let margin = { top: 40, right: 100, bottom: 105, left: 70 },
            width = 960 - margin.left - margin.right,
            height = 500 - margin.top - margin.bottom;

        // Append the svg object to the element with bar-graph id
        let windowOfDowntimeGraph = d3.select("#bar-graph")
            .append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        // X axis Label 
        windowOfDowntimeGraph
            .append("text")
            .attr(
                "transform",
                "translate(" + width / 2 + " ," + (height + margin.top + 50) + ")"
            )
            .style("text-anchor", "middle")
            .text("Date (Y-M-D) when downtime started");

        // Y axis Label
        windowOfDowntimeGraph
            .append("text")
            .attr("transform", "rotate(-90)")
            .attr("y", 0 - margin.left)
            .attr("x", 0 - height / 2)
            .attr("dy", "1em")
            .style("text-anchor", "middle")
            .text("Downtime(days)");
        
        // Graph title
        windowOfDowntimeGraph
            .append("text")
            .attr("x", width / 2)
            .attr("y", 0 - margin.top / 2)
            .attr("text-anchor", "middle")
            .style("font-size", "20px")
            .style("text-decoration", "bold")
            .text("Periods with the maximum window of time with 0 messages.");

        d3.json(data_path).then(window_of_downtime => {
            let data = window_of_downtime.filter(d => +d.DownTimeDurationSeconds > threshold);
            data.forEach(d => {
                d.date = new Date(d.PreviousMessageTimestamp);
                d.endDate = new Date(d.NextMessageTimestamp)
                d.value = +d.DownTimeDurationSeconds;
            });
            // Create a date formatter to display a shorter date
            const formatDate = d3.timeFormat("%Y-%m-%d");

            // X axis
            let x = d3.scaleBand()
                .range([ 0, width ])
                .domain(data.map(d => d.date))
                .padding(0.2);

            let xAxis = d3.axisBottom(x)
                .tickFormat(formatDate)
                .tickValues(x.domain().filter((d,i) => !(i%10))) // control d3 scaleBand ticks
                // tickValues uses the remainder operator to show only 1 in every 10 ticks

            windowOfDowntimeGraph.append("g")
                .attr("transform", "translate(0," + height + ")")
                .call(xAxis)
                .selectAll("text")
                .attr("transform", "translate(-10,0)rotate(-45)")
                .style("text-anchor", "end");

            // Add Y axis
            let y = d3.scaleLinear()
                .domain(d3.extent(data, d => d.value))
                .range([ height, 0]);
            let decimalFormatter = d3.format(".2s");
            windowOfDowntimeGraph.append("g").call(
                d3.axisLeft(y).ticks(10).tickFormat(d => decimalFormatter(d / 86400)));

            // Bars
            windowOfDowntimeGraph.selectAll("mybar")
                .data(data)
                .enter()
                .append("rect")
                .attr("x", d => x(d.date))
                .attr("y", d => y(d.value))
                .attr("width", x.bandwidth())
                .attr("height", d => height - y(d.value))
                .attr("fill", "#69b3a2")
                .on("mouseover", function(d){
                    let xPoint = parseFloat(d3.select(this).attr('x')) + x.bandwidth() / 2;
                    let yPoint = parseFloat(d3.select(this).attr('y')) / 2 + height / 2;
                    let seconds = parseInt(d.value, 10);
                    let days = Math.floor(seconds / (3600*24));
                    seconds -= days*3600*24;
                    let hours = Math.floor(seconds / 3600);
                    seconds -= hours*3600;
                    let minutes = Math.floor(seconds / 60);
                    seconds -= minutes*60;
                    d3.select("#tooltip")
                        .style("left", xPoint + "px")
                        .style("top", yPoint + "px")
                        .style("display", "block")
                        .text(`From: ${d3.timeFormat("%Y-%m-%d (%H:%M)")(d.date)}
                            To: ${d3.timeFormat("%Y-%m-%d (%H:%M)")(d.endDate)}
                            Downtime: ${days}d ${hours}h ${minutes}m ${seconds}s`);
                })
                .on("mouseout", () => d3.select("#tooltip").style("display", "none"));
        }, err => {
            console.log(err)
        });
    } 
} 
