function hideFields() {
    $(".results-modal").hide();
        $("#generic_template_container").hide()
        $("#report_owner_container").hide()
        $("#insurance_carrier_container").hide()
        $("#month_container").hide()
        $("#report_name_container").hide()
        $("#year_container").hide()
       $(".confirm-btn").hide()
    $(".warning-text").hide()
}

// Makes an api call to zoho to get insurance carrier field
async function getCarrierField() {
  try {
    const data = await ZOHO.CRM.META.getFields({ Entity: "Carrier_Reports" });

    let carrierArray = [];

    let carrierObject = data.fields.find(field => field.api_name === "Carrier");
    let values = carrierObject.pick_list_values;

    for (let i = 0; i < values.length; i++) {
      carrierArray.push(values[i].display_value);
    }
    const selectElement = document.getElementById("insurance_carrier");

    carrierArray.forEach(carrier => {
      const optionElement = document.createElement("option");
      optionElement.value = carrier;
      optionElement.textContent = carrier;
      selectElement.appendChild(optionElement);
    });
  } catch (error) {
    console.log(error);
  }
}

  // Makes an api call to zoho to get field data for Owner field and returns the owner map
async function getReportOwnerField() {
  try {
    const data = await ZOHO.CRM.API.getAllUsers({Type:"ActiveUsers"});
    let values = data.users;
    
    let ownerArray = [];
    let ownerMap = {};

    for (let i = 0; i < values.length; i++) {
      const { full_name, id } = values[i];
      ownerMap[full_name] = id;
      ownerArray.push(full_name);
    }

    const selectElement = document.getElementById("report_owner");

    ownerArray.forEach(owner => {
      const optionElement = document.createElement("option");
      optionElement.value = owner;
      optionElement.textContent = owner;
      selectElement.appendChild(optionElement);
    });

    return ownerMap;
  } catch (error) {
    console.log(error);
  }
}









