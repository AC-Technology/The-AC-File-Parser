function addReportNameOptions(fileName) {
  
  $("#report_name").empty()
  $("#report_name").append(new Option("-Select-", ""))
  $("#report_name").val("-Select-");
  let hasReportName = true;
  switch ($("#insurance_carrier").val()) {
    case "Guard":
      $("#report_name").append([
        new Option("Calendar Year", "Calendar Year"),
        new Option("Plan Year", "Plan Year"),
        new Option("No Report Name", "")
      ]);
      // hasReportName = false;
      break;

    case "Liberty Mutual":
      if (fileName.includes(".pdf")) {
        $("#report_name").append(
          [new Option("YE Results", "YE Results"),
          new Option("No Report Name", "")
      ]);
      } else {
        hasReportName = false;
      }
      break;

    case "Nationwide":
      if (fileName.includes(".pdf")) {
        $("#report_name").append([
          new Option("All In", "All In"),
          new Option("All Codes", "All Codes"),
          new Option("Standard Contract", "Standard Contract"),
          new Option("Elite Contract", "Elite Contract"),
          new Option("No Report Name", "")
        ]);
      } else {
        hasReportName = false;
      }
      break;
    
    case "Safeco":
      if (fileName.includes(".pdf")) {
        $("#report_name").append([new Option("YE Results", "YE Results"),
        new Option("No Report Name", "")
      ]);
      } else {
        hasReportName = false;
      }
      break;

    case "State Auto":
      hasReportName = false;
      if (fileName.includes(".pdf")) {
        $("#report_name").append([
          new Option("Personal", "Personal"),
          new Option("Commercial", "Commercial"),
          new Option("Farm", "Farm"),
          new Option("Production & Loss Summary", "Production & Loss Summary"),
          new Option("No Report Name", "")
        ]);
      } else {
        hasReportName = false;
      }
      break;

    case "Selective":
      $("#report_name").append([
        new Option("YTD", "YTD"),
        new Option("No Report Name", "")
      ]);
      break;
    
    case "Travelers":
      $("#report_name").append([
        new Option("Agency Selection", "Agency Selection"),
        new Option("Agency Summary", "Agency Summary"),
        new Option("Sub-Code", "Sub-Code"),
        new Option("Production Data Summary", "Production Data Summary"),
        new Option("YE Results", "YE Results"),
        new Option("No Report Name", "")
      ]);
      break;

    case "Wyandot Mutual":
      $("#report_name").append([
        new Option("YE Results", "YE Results"),
        new Option("No Report Name", "")
    ]);
      break;

    default:
      hasReportName = false;
      break;
  }
  if (hasReportName) {
    $("#report_name_container").fadeIn();
  } else {
    $("#month_container").fadeIn();
  }
  return hasReportName;
}
