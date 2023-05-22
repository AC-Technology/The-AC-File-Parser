function resetFields(){
    // $(".confirm-btn").prop("disabled",false)
    $(".drop-box-text").text("Drag & Drop")
    $(".drop-box-text2").text("Select File")
    $(".drop-box-text3").text("or");
    $("#insurance_carrier_container").hide()
    $("#month_container").hide()
    $("#report_name_container").hide()
    $("#year_container").hide()
    $("#report_owner_container").hide()
    $("#generic_template_container").hide()
    $(".confirm-btn").hide()
    $("#insurance_carrier").val("-Select-")
    $("#report_month").val("-Select-")
    $("#report_year").val("-Select-")
    $("#generic_template").val("-Select-")
    $("#report_owner").val("-Select-")
    $(".results-modal").fadeOut();
  
}