// functions
function changeDropText(event) {
  // prevent default action
  event.preventDefault();
  // change drop text when user is dragging file over the screen
  $(".drop-box-text").text("Drop Here");
}

function selectFile() {
  $("#fileInput").trigger("click");
}

function processSelectedFile(event) {
  event.preventDefault();


    let fileName = event.target.files[0].name;
    let file = event.target.files[0];
    let fileType;
    $(".browse-button").text(fileName);
    $(".drop-box-text").text("File Uploaded");
    $(".drop-box-text3").hide();
    $("#generic_template_container").fadeIn();
    if (fileName.includes(".xlsx")) {
      fileType = '.xlsx'

    } else if (fileName.includes(".pdf")) {
      fileType = '.pdf'

    }
    return {
      fileName,
      file,
      fileType
    };
}

function processDroppedFile(event) {
  event.preventDefault();
  let file = event.originalEvent.dataTransfer.files[0];
  let fileName = event.originalEvent.dataTransfer.files[0].name;
  $(".drop-box-text").text("File Uploaded");
  $(".browse-button").text(file.name);
  $(".drop-box-text3").hide();
  $("#generic_template_container").fadeIn();
  if (fileName.includes(".xlsx")) {
    fileType = '.xlsx'

  } else if (fileName.includes(".pdf")) {
    fileType = '.pdf'

  }

  return {
    fileName,
    file,
    fileType
  };
}