import json
import boto3

sns = boto3.client('sns')

SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:590183965989:AnomalyAlerts'

def lambda_handler(event, context):
    for record in event['Records']:
        if record['eventName'] in ['INSERT', 'MODIFY']:
            new_image = record['dynamodb'].get('NewImage', {})
            old_image = record['dynamodb'].get('OldImage', {})

          
            patient_id = new_image.get('PatientID', {}).get('S', 'Unknown')
            timestamp = new_image.get('Timestamp', {}).get('S', 'Unknown')
            blood_pressure = new_image.get('BloodPressure', {}).get('S', 'N/A')
            heart_rate = new_image.get('HeartRate', {}).get('S', 'N/A')
            oxygen_saturation = new_image.get('OxygenSaturation', {}).get('S', 'N/A')

            
            message = f"Patient ID: {patient_id}\nTimestamp: {timestamp}\n"

            if record['eventName'] == 'MODIFY':
                old_blood_pressure = old_image.get('BloodPressure', {}).get('S', 'N/A')
                if blood_pressure != old_blood_pressure:
                    message += f"Blood Pressure changed from {old_blood_pressure} to {blood_pressure}\n"

                old_heart_rate = old_image.get('HeartRate', {}).get('S', 'N/A')
                if heart_rate != old_heart_rate:
                    message += f"Heart Rate changed from {old_heart_rate} to {heart_rate}\n"

                old_oxygen_saturation = old_image.get('OxygenSaturation', {}).get('S', 'N/A')
                if oxygen_saturation != old_oxygen_saturation:
                    message += f"Oxygen Saturation changed from {old_oxygen_saturation} to {oxygen_saturation}\n"

            if message.strip():  # Only send if there's a message to send
                response = sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=message,
                    Subject='DynamoDB Data Change Alert'
                )

    return {
        'statusCode': 200,
        'body': json.dumps('Processed DynamoDB stream events successfully')
    }
