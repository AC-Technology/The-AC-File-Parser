$(document).ready(async function () {
  // hideFields()

  let file;
  let fileName;
  let fileType;
  let reportOwner;

  hideFields();

  ZOHO.embeddedApp.on("PageLoad", async function(data) {

  // call getCarrierField function to grab the carriers to update the carrier pick list
  getCarrierField();

  // call getReportOwnerField function to grab the carriers to update the report owner pick list and store the hashmap of the user ids 
  let ownerMap = await getReportOwnerField();
  console.log('map',ownerMap);

  // drag and drop controls
  $(document).on("dragover", function (event) {
    changeDropText(event);
  });
  $(".drop-box-text2").click(function () {
    selectFile();
  });
  $(".drag-drop-area").on("drop", async function (event) {
    fileObj = processDroppedFile(event);
    fileName = fileObj.fileName;
    file = fileObj.file;
    fileType = fileObj.fileType
  });
  $("#fileInput").change(async function (event) {
    fileObj = processSelectedFile(event);
    if (fileObj) {
      fileName = fileObj.fileName;
      file = fileObj.file;
      fileType = fileObj.fileType;
    }
  });


  // drop down controls
  let hasReportName;
  $("#generic_template").change(function () {
    $("#report_owner_container").fadeIn()
  })

  $("#report_owner").change(function () {
    $("#insurance_carrier_container").fadeIn()
  })
  
  $("#insurance_carrier").change(function () {
    hasReportName = addReportNameOptions(fileName);
  });
  $("#report_name").change(function () {
    $("#month_container").fadeIn()
  })
  $("#report_month").change(function () {
    $("#year_container").fadeIn()
  })
  $("#report_year").change(function () {
    $(".confirm-btn").fadeIn()
  })


  // button contols
  $(".confirm-btn").click(async function () {

    // hide the confirm button
    $(this).hide();

    // grab the fileName created
    fileName = getReportName(hasReportName,fileType);

    // grab the report owner and then get the id
    reportOwner = $("#report_owner").val();
    let userID = ownerMap[reportOwner];

    if (fileType =='.xlsx') {
      submitExcelFile(fileName,file,userID);
    }
    if (fileType=='.pdf') {
      submitPdfFile(fileName,file,userID);
    }
  });

  $(".close-modal-btn").click(function () {
    resetFields()

  })

  })
  ZOHO.embeddedApp.init();

});