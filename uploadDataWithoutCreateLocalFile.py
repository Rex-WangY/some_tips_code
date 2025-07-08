def write_item(sharepoint, df: pd.DataFrame, **kwargs):
    
    site_id, list_id = sharepoint.check_group(**kwargs)
    if not kwargs.get('fileName') and not kwargs.get('parentFolderName') and not kwargs.get('fileURL'):
        return False

    header = sharepoint.get_token()
    fileName = kwargs.get('fileName')
    parentFolderName = kwargs.get('parentFolderName')

    if kwargs.get('fileURL'):
        parentFolderName, fileName = kwargs.get('fileURL').rsplit('/', 1)
    
    # Step 2: Convert DataFrame to a list of lists
    data = df.values.tolist()


    # Step 3: Clear the existing content in the Excel workbook (clear the calculated range)
    clear_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{parentFolderName}/{fileName}:/workbook/worksheets('Sales List Fixed')/clear"
    clear_response = requests.post(clear_url, headers=header)
    
    if clear_response.status_code != 204:  # No Content response indicates success
        return False, f"Failed to clear the existing content. Status code: {clear_response.status_code}, Message: {clear_response.text}"

    # Step 4: Write the new DataFrame data into the Excel workbook, including the header
    update_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{parentFolderName}/{fileName}:/workbook/worksheets('Sales List Fixed')"
    body = {
        "values": data
    }
    
    update_response = requests.patch(update_url, headers={**header, "Content-Type": "application/json"}, data=json.dumps(body))

    if update_response.status_code not in [200, 201]:
        return False
    else:
        return True


    sharepoint = microsoft_sharepoint(
        client_id = os.getenv('client_id'),
        client_secret = os.getenv('client_secret'),
        tenant_id = os.getenv('tenant_id'),
        site_id = os.getenv('site_id'),
        list_id = os.getenv('list_id')
    )

    param = {
        'fileURL': 'url'.format(fileName),
        'fileLocated': ''
    }
    result = write_item(sharepoint, salesResult, **param)
