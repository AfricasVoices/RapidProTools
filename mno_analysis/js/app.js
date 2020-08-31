import { MessageDifference } from "./msg_difference_btwn_two_firebase_time_periods_graph.js";
import { WindowOfDowntime } from "./window_of_downtime_graph.js";

// GLOBAL APP CONTROLLER
class Controller {
    static updateGraphs(windowOfDowntimeThreshold = 1) {
        if (!Controller.msgDowntimeDataPath) Controller.msgDowntimeDataPath = "./data/incoming_messages/incoming_msg_downtime.json";
        if (!Controller.msgDifferenceDataPath) Controller.msgDifferenceDataPath = "./data/incoming_messages/incoming_msg_diff_per_period.json";
        // Clear previous graphs before redrawing
        d3.selectAll("svg").remove();
        WindowOfDowntime.drawGraph(Controller.msgDowntimeDataPath, windowOfDowntimeThreshold);
        MessageDifference.drawGraph(Controller.msgDifferenceDataPath);
    }
}
Controller.updateGraphs();

// Update chart data
d3.select("#in").on("click", () => {
    Controller.msgDowntimeDataPath = "./data/incoming_messages/incoming_msg_downtime.json";
    Controller.msgDifferenceDataPath = `./data/incoming_messages/incoming_msg_diff_per_period.json`;
    Controller.updateGraphs();
});
d3.select("#out").on("click", () => {
    Controller.msgDowntimeDataPath = "./data/outgoing_messages/outgoing_msg_downtime.json";
    Controller.msgDifferenceDataPath = `./data/outgoing_messages/outgoing_msg_diff_per_period.json`;
    Controller.updateGraphs();
});
d3.select("#downtimeThreshold").on("input", function() {
    // WindowOfDowntime.drawGraph(msgDowntimeDataPath, this.value);
    Controller.updateGraphs(this.value);
});
