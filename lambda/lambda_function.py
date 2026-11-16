import json
import os
import boto3
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import unquote_plus
from pathlib import PurePosixPath

s3 = boto3.client("s3")
sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def lambda_handler(event, context):

    processed = []

    for record in event.get("Records", []):

        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        # Process only invoice JSON files
        if not key.startswith("invoice/") or not key.endswith(".json"):
            continue

        # Read invoice from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        invoice = json.loads(response["Body"].read().decode("utf-8"))

        # Get invoice amount
        amount = Decimal(str(invoice["amount"]))

        # Calculate 10% discount for orders of Rs. 5000 or more
        if amount >= Decimal("5000"):
            discount = (amount * Decimal("0.10")).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        else:
            discount = Decimal("0.00")

        # Calculate taxable amount
        taxable_amount = amount - discount

        # Calculate 18% GST on taxable amount
        gst = (taxable_amount * Decimal("0.18")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        # Calculate final total
        total = taxable_amount + gst

        # Create invoice summary
        summary = {
            "invoice_id": invoice["invoice_id"],
            "customer": invoice["customer"],
            "email": invoice["email"],
            "item": invoice["item"],
            "quantity": invoice["quantity"],
            "unit_price": invoice["unit_price"],
            "subtotal": float(amount),
            "discount": float(discount),
            "taxable_amount": float(taxable_amount),
            "gst_rate": "18%",
            "gst_amount": float(gst),
            "total_with_gst": float(total)
        }

        # Save summary in processed/ folder
        filename = PurePosixPath(key).stem
        summary_key = f"processed/{filename}-summary.json"

        s3.put_object(
            Bucket=bucket,
            Key=summary_key,
            Body=json.dumps(summary, indent=2).encode("utf-8"),
            ContentType="application/json"
        )

        # Send email notification
        message = (
            f"Invoice processed successfully!\n\n"
            f"Invoice ID: {invoice['invoice_id']}\n"
            f"Customer: {invoice['customer']}\n"
            f"Subtotal: Rs. {amount}\n"
            f"Discount: Rs. {discount}\n"
            f"GST (18%): Rs. {gst}\n"
            f"Total: Rs. {total}\n\n"
            f"Summary saved in S3: {summary_key}"
        )

        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"Invoice Processed: {invoice['invoice_id']}",
            Message=message
        )

        processed.append(summary_key)

        print(f"Successfully processed: {key}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Invoice processing completed",
            "processed_files": processed
        })
    }