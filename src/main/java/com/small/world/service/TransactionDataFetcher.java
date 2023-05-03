package com.small.world.service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.small.world.model.Transaction;

/**
 * This TransactionDataFetcher class is a component that allows us to get some
 * insight into the transactions.
 * 
 * @author Ayub Ahmed
 *
 */
public class TransactionDataFetcher {

	private static final ObjectMapper mapper = new ObjectMapper();
	private static File importFile = new File("transactions.json");
	private static TransactionDataFetcher data = new TransactionDataFetcher();

	public static void main(String[] args) {

		// ---------------------------------------------------
		// Since we already loaded transactions battery
		// so we are not writing unit test for them
		// now we are just calling TransactionDataFetcher functions
		// ---------------------------------------------------
		try {
			List<Transaction> transactions = mapper.readValue(importFile, new TypeReference<List<Transaction>>() {
			});
			data.getTotalTransactionAmount(transactions);
			data.getTotalTransactionAmountSentBy("Tom Shelby", transactions);
			data.getMaxTransactionAmount(transactions);
			data.countUniqueClients(transactions);
			data.hasOpenComplianceIssues("Alfie Solomons", transactions);
			data.getTransactionsByBeneficiaryName(transactions);
			data.getUnsolvedIssueIds(transactions);
			data.getAllSolvedIssueMessages(transactions);
			data.getTop3TransactionsByAmount(transactions);
			data.getTopSender(transactions);
		} catch (IOException e) {
			System.out.println("Exception occurred while mapping values to list.");
			System.out.println(e);
		}
	}

	/**
	 * Returns the sum of the amounts of all transactions
	 */
	public double getTotalTransactionAmount(List<Transaction> transactions) {

		if (!transactions.isEmpty()) {
			Double totalTransactionsAmount = transactions.stream().mapToDouble(p -> p.getAmount()).sum();
			System.out.println("Total Transaction Amount is " + totalTransactionsAmount);
			return totalTransactionsAmount;
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Returns the sum of the amounts of all transactions sent by the specified
	 * client
	 */
	public double getTotalTransactionAmountSentBy(String senderFullName, List<Transaction> transactions) {

		if (!transactions.isEmpty()) {

			Double totalTransactionsAmount = transactions.stream()
					.filter(p -> p.getSenderFullName().equalsIgnoreCase(senderFullName)).mapToDouble(p -> p.getAmount())
					.sum();
			System.out.println("Total Transaction Amount Sent By " + senderFullName + " is " + totalTransactionsAmount);
			return totalTransactionsAmount;
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Returns the highest transaction amount
	 */
	public double getMaxTransactionAmount(List<Transaction> transactions) {

		if (!transactions.isEmpty()) {
			Optional<Transaction> maxNumber = transactions.stream()
					.max((i, j) -> i.getAmount().compareTo(j.getAmount()));

			System.out.println("Maximum Transaction Amount is " + maxNumber.get().getAmount());
			return maxNumber.get().getAmount();
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Counts the number of unique clients that sent or received a transaction
	 */
	public long countUniqueClients(List<Transaction> transactions) {

		if (!transactions.isEmpty()) {
			List<String> uniqueClients = new ArrayList<>();

			for (int i = 0; i < transactions.size(); i++) {

				if (!uniqueClients.contains(transactions.get(i).getSenderFullName())) {
					uniqueClients.add(transactions.get(i).getSenderFullName());
				}

				if (!uniqueClients.contains(transactions.get(i).getBeneficiaryFullName())) {
					uniqueClients.add(transactions.get(i).getBeneficiaryFullName());
				}
			}

			System.out.println("Unique Clients count is " + uniqueClients.size());
			return uniqueClients.size();
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Returns whether a client (sender or beneficiary) has at least one transaction
	 * with a compliance issue that has not been solved
	 */
	public boolean hasOpenComplianceIssues(String clientFullName, List<Transaction> transactions) {

		if (!transactions.isEmpty()) {
			List<Transaction> openIssuesTransactions = transactions.stream()
					.filter(p -> p.getSenderFullName().equalsIgnoreCase(clientFullName)
							|| p.getBeneficiaryFullName().equalsIgnoreCase(clientFullName))
					.filter(p -> p.getIssueId() != null && p.getIssueSolved() == false).collect(Collectors.toList());

			System.out.println(clientFullName + " has " + openIssuesTransactions.size() + " Open Compliance Issues");
			return openIssuesTransactions.isEmpty();
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Returns all transactions indexed by beneficiary name
	 */
	public Map<String, List<Transaction>> getTransactionsByBeneficiaryName(List<Transaction> transactions) {

		if (!transactions.isEmpty()) {
			Map<String, List<Transaction>> result = new HashMap<>();

			List<Transaction> transactionResult;
			for (int j = 0; j < transactions.size(); j++) {
				transactionResult = new ArrayList<>();
				for (int i = j; i < transactions.size(); i++) {

					if (result.containsKey(transactions.get(i).getBeneficiaryFullName())) {
						break;
					}
					if (!result.containsKey(transactions.get(i).getBeneficiaryFullName())) {
						if (transactions.get(i).getBeneficiaryFullName()
								.equals(transactions.get(j).getBeneficiaryFullName())) {
							transactionResult.add(transactions.get(i));
						}
					}
				}
				if (!transactionResult.isEmpty())
					result.put(transactions.get(j).getBeneficiaryFullName(), transactionResult);
			}
			return result;
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Returns the identifiers of all open compliance issues
	 */
	public Set<Integer> getUnsolvedIssueIds(List<Transaction> transactions) {

		if (!transactions.isEmpty()) {
			Set<Integer> unsolvedIssues = transactions.stream()
					.filter(p -> p.getIssueId() != null && p.getIssueSolved() == false).map(Transaction::getMtn)
					.collect(Collectors.toSet());

			System.out.println("Unsolved issues ids are " + unsolvedIssues.size());

			return unsolvedIssues;
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Returns a list of all solved issue messages
	 */
	public List<String> getAllSolvedIssueMessages(List<Transaction> transactions) {

		if (!transactions.isEmpty()) {
			List<String> solvedMessages = transactions.stream()
					.filter(p -> p.getIssueSolved() == true && p.getIssueId() != null).map(Transaction::getIssueMessage)
					.collect(Collectors.toList());
			System.out.println("Solved Issue Messages are of size " + solvedMessages.size());
			return solvedMessages;
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Returns the 3 transactions with highest amount sorted by amount descending
	 */
	public List<Transaction> getTop3TransactionsByAmount(List<Transaction> transactions) {// We have two transaction in
																							// top 3 with same result
		// assuming the requirement is not for unique
		if (!transactions.isEmpty()) {
			transactions.sort(Comparator.comparing(Transaction::getAmount));
			Collections.reverse(transactions);
			List<Transaction> subList = transactions.subList(0, 3);
			System.out.println("Top 3 Transaction amounts are as follows");
			for (Transaction i : subList)
				System.out.println("Transaction amount is " + i.getAmount());
			return subList;

		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Returns the sender with the most total sent amount
	 */
	public String getTopSender(List<Transaction> transactions) {

		if (!transactions.isEmpty()) {
			Map<String, Double> result = new HashMap<>();
			String senderCheck = "";

			Double amountSum;
			for (int j = 0; j < transactions.size(); j++) {
				amountSum = 0.0;
				for (int i = j; i < transactions.size(); i++) {

					if (result.containsKey(transactions.get(i).getSenderFullName())) {
						break;
					}
					if (!result.containsKey(transactions.get(i).getSenderFullName())) {
						senderCheck = transactions.get(j).getSenderFullName();
						if (transactions.get(i).getSenderFullName().equals(senderCheck)) {

							amountSum += transactions.get(i).getAmount();
						}
					}
				}
				if (amountSum > 0) {
					result.put(senderCheck, amountSum);
				}
			}

			Map.Entry<String, Double> maxEntry = null;
			for (Map.Entry<String, Double> entry : result.entrySet()) {
				if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0) {
					maxEntry = entry;
				}
			}
			System.out.println(
					"Top Sender is " + maxEntry.getKey() + " and transaction amount is " + maxEntry.getValue());
			return null;

		} else {
			throw new UnsupportedOperationException();
		}
	}

}
