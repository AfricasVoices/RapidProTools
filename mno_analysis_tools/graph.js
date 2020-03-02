// Set the dimensions and margins of the graphs
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