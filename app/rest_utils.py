import numpy as np
import pandas as pd
import calendar


def calculatePortfolioBeta(weights: list, betas: list) -> float:
    portfolioBeta = np.dot(weights, betas)

    return portfolioBeta


def calculateSystematicCovarianceMatrix(
    betas: list, marketVolatility: float
) -> np.ndarray:
    betasColumn = np.array(np.array(betas)[np.newaxis, :])
    betasRow = betasColumn.transpose()

    systematicCovarianceMatrixMatrix = (
        np.matmul(betasRow, betasColumn) * marketVolatility ** 2
    )

    return systematicCovarianceMatrixMatrix


def calculateSpecificCovarianceMatrix(specificVolatility: list) -> np.ndarray:
    specificVolatilitySquared = np.diag(np.square(np.array(specificVolatility)))
    return specificVolatilitySquared


def calculatePortfolioSystematicVariance(
    weights: list, betas: list, marketVolatility: float
) -> float:
    weightsTranspose = np.array(weights).transpose()
    betasTranspose = np.array(betas).transpose()
    portfolioSystematicVariance = np.dot(
        np.matmul(weightsTranspose, betas) * (np.matmul(betasTranspose, weights)),
        (marketVolatility ** 2),
    )

    return np.asscalar(portfolioSystematicVariance)


def calculatePortfolioSpecificVariance(
    weights: list, specificVolatility: list
) -> float:
    weightsColumn = np.array(np.array(weights)[np.newaxis, :])
    weightsRow = weightsColumn.transpose()
    specificVolatilitySquared = calculateSpecificCovarianceMatrix(specificVolatility)
    portfolioSpecificVariance = np.matmul(
        weightsColumn, np.matmul(specificVolatilitySquared, weightsRow)
    )

    return np.asscalar(portfolioSpecificVariance)


def calculateTotalCovarianceMatrix(
    systematicCovarianceMatrix: np.ndarray, specificCovarianceMatrix: np.ndarray
) -> np.ndarray:
    totalCovarianceMatrix = systematicCovarianceMatrix + specificCovarianceMatrix

    return totalCovarianceMatrix


def calculatePortfolioVariance(
    portfolioSystematicVariance: float, portfolioSpecificVariance: float
) -> float:
    portfolioVariance = portfolioSystematicVariance + portfolioSpecificVariance

    return portfolioVariance


def calculateCorrelationMatrix(totalCovarianceMatrix: np.ndarray) -> np.ndarray:
    D = np.diag(np.sqrt(np.diag(totalCovarianceMatrix)))
    inverseD = np.linalg.inv(D)
    correlationMatrix = np.matmul(np.matmul(inverseD, totalCovarianceMatrix), inverseD)
    return correlationMatrix


def getLastDayOfMonth(date) -> int:
    year = int(date[0:4])
    month = int(date[5:7])
    lastDayOfMonth = calendar.monthrange(year, month)[1]
    return year, month, lastDayOfMonth


def createEndDate(date):
    year, month, lastDayOfMonth = getLastDayOfMonth(date)
    endDate = "-".join([str(year), str(month), str(lastDayOfMonth)])
    return endDate


def calculateStatistics(
    betas: list,
    weights: list,
    specificVolatility: list,
    marketVolatility: list,
):
    portfolioBeta = calculatePortfolioBeta(
        weights,
        betas,
    )
    systematicCovarianceMatrix = calculateSystematicCovarianceMatrix(
        betas, marketVolatility
    )
    portfolioSystematicVariance = calculatePortfolioSystematicVariance(
        weights, betas, marketVolatility
    )
    specificCovarianceMatrix = calculateSpecificCovarianceMatrix(specificVolatility)
    portfolioSpecificVariance = calculatePortfolioSpecificVariance(
        weights, specificVolatility
    )
    totalCovarianceMatrix = calculateTotalCovarianceMatrix(
        systematicCovarianceMatrix, specificCovarianceMatrix
    )
    portfolioVariance = calculatePortfolioVariance(
        portfolioSystematicVariance, portfolioSpecificVariance
    )
    correlationMatrix = calculateCorrelationMatrix(totalCovarianceMatrix)

    return (
        portfolioBeta,
        specificCovarianceMatrix,
        systematicCovarianceMatrix,
        portfolioSpecificVariance,
        portfolioSystematicVariance,
        totalCovarianceMatrix,
        portfolioVariance,
        correlationMatrix,
    )
