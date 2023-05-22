function getReportName(hasReportName,fileType) {
  let fileName;
  if (hasReportName) {
    if($("#report_name").val()=='None'){
      alert('Enter Report Name')
      return
    }
    fileName = `${$("#insurance_carrier").val()} (${$( "#report_name").val()}) - ${$("#report_month").val()} ${$("#report_year").val()}${fileType}`;
  } 
  else {
    fileName = `${$("#insurance_carrier").val()} - ${$("#report_month").val()} ${$("#report_year").val()}${fileType}`;
  }
  console.log(fileName)
  return fileName
}

async function submitExcelFile(fileName, file, userID) {
  let insuranceCarrier = document.getElementById("insurance_carrier").value;
  var carrier_data = {
    uploadedFileName: file.name,
    carrier: insuranceCarrier,
    name: fileName,
    userID: userID
  };

  // get presigned url

  var response = await $.ajax({
    method: "POST",
    url: "https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/PresignedExcelURLs",
    data: carrier_data,
  });

  console.log(response);

  S3_URL = response["uploadURL"];
  key = response["Key"];

  // upload file to s3 using presigned url
  var s3UploadResp = await $.ajax({
    method: "PUT",
    headers: {
      "Content-Type":
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    },
    url: S3_URL,
    processData: false,
    data: file,
  });

  // start parser based on carrier
  if($("#generic_template").val() == 'No'){
    console.log("This file does not use the generic template")
    switch (insuranceCarrier) {
      case "Attune":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/AttuneParser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "Foremost":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/ForemostExcelParser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "Grange":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/GrangeParser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "Guard":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/GuardParser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "Kemper":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/KemperParser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "Liberty Mutual":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/LibertyMutualParser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "Main Street America":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/MainStreetAmericaParser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "Mercury":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/MercuryParser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "Nationwide":
        try {
          console.log(userID)
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/AC_File_Parser?carrier=Nationwide&filename=${fileName}&key=${key}&userID=${userID}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "Ohio Mutual":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/OhioMutualParser?filename=${fileName}&key=${key}&userID=${userID}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "Progressive":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/AC_File_Parser?carrier=Progressive&filename=${fileName}&key=${key}&userID=${userID}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "Safeco":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/AC_File_Parser?carrier=Safeco&filename=${fileName}&key=${key}&userID=${userID}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "State Auto":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/StateAutoParser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "National General":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/National_General_Parser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "USLI":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/USLI_Parser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      case "Allstate":
        try {
          $(".results-modal").fadeIn()
          var lambdafunction = await $.ajax({
            method: "GET",
            url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/Allstate_Parser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
          });
          console.log("success");
        } catch (error) {
          console.log(error);
        }
        break;
      default:
        alert("Sorry! There is currently no parser for that file format. Please contact us to create a parser for that file.");
    }
  }
  else if ($("#generic_template").val() == 'Yes'){
    console.log("This file does use the generic template")
    try {
      $(".results-modal").fadeIn()
      var lambdafunction = await $.ajax({
        method: "GET",
        url: `https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/Generic_Parser?filename=${fileName}&key=${key}&userID=${userID}&uploadedFileName=${encodeURIComponent(file.name)}`,
      });
      console.log("success");
    } catch (error) {
      console.log(error);
    }
  }
  else{
    console.log($("#generic_template").val())
    console.log("There was an error or a option was not slected for the generic template")
  }
}
  
async function submitPdfFile(fileName, file, userID) {
  let insuranceCarrier = document.getElementById("insurance_carrier").value;
  console.log(insuranceCarrier);
  console.log(fileName);

  var carrier_data = {
    uploadedFileName: file.name,
    carrier: insuranceCarrier,
    name: fileName,
    userID: userID
  };

  var response = await $.ajax({
    method: "POST",
    url: "https://cv7022xmr7.execute-api.us-east-1.amazonaws.com/default/PresignedPDFUrls",
    data: carrier_data,
  });

  console.log(response);

  S3_URL = response["uploadURL"];
  key = response["Key"];

  // upload file to s3
  var s3UploadResp = await $.ajax({
    method: "PUT",
    headers: {
      "Content-Type": "application/pdf",
    },
    url: S3_URL,
    processData: false,
    data: file,
  });
  $(".results-modal").fadeIn()

}
