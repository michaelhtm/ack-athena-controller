	if ko.Status.ACKResourceMetadata != nil {
		// We need to build the resourceARN from accountID and region,
		// since it is not directly returned by the API.
		resourceARN := ackv1alpha1.AWSResourceName(fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s",
		*ko.Status.ACKResourceMetadata.Region, *ko.Status.ACKResourceMetadata.OwnerAccountID, *ko.Spec.Name))

		// Set resourceARN to status
		ko.Status.ACKResourceMetadata.ARN = &resourceARN

		// Now we can fetch the tags using the manually constructed ARN
		tags, err := rm.getTags(ctx, string(resourceARN))
		if err != nil {
			return nil, err
		}
		ko.Spec.Tags = tags
	}
