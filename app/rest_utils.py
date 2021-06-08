import numpy as np
import calendar


def calculatePortfolioBeta(weightsList: list, betasList: list):
    weights = np.array(weightsList)
    weightsTranspose = np.transpose(weights)
    betas = np.array(betasList)
    portfolioBeta = weightsTranspose * betas
    return portfolioBeta


def calculateSystematicCovariance(betasList: list):
    betas = np.array(betasList)
    betasTranspose = np.transpose(betas)
    m = 0
    systematicCovarianceMatrix = betas * betasTranspose * m ^ 2
    return systematicCovarianceMatrix


def calculatePortfolioSystematicVariance(weightsList: list, betasList: list):
    weights = np.array(weightsList)
    weightsTranspose = np.transpose(weightsList)
    betas = np.array(betasList)
    betasTranspose = np.transpose(betasList)
    m = 0
    portfolioSystematicVariance = (
        weightsTranspose * betas * betasTranspose * weights * m ^ 2
    )
    return portfolioSystematicVariance


def calculateSpecificCovariance(specificVolatility: list):
    specificVolatilitySquared = np.diag(specificVolatility) ^ 2
    return specificVolatilitySquared


def calculatePortfolioSpecificVariance(weightsList: list, specificVolatility: list):
    weights = np.array(weightsList)
    weightsTranspose = np.transpose(weights)
    specificVolatilitySquared = calculateSpecificCovariance(specificVolatility)
    portfolioSpecificVariance = weightsTranspose * specificVolatilitySquared * weights
    return portfolioSpecificVariance


def calculateTotalCovariance(systematicCovariance, specificCovariance):
    totalCovariance = systematicCovariance + specificCovariance
    return totalCovariance


def calculatePortfolioVariance(
    portfolioSystematicCovariance, portfolioSpecificCovariance
):
    portfolioVariance = portfolioSystematicCovariance + portfolioSpecificCovariance
    return portfolioVariance


def calculateCorrelationMatrix(weightsList: list, betasList: list):
    weights = np.array(weightsList)
    betas = np.array(betasList)


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
):
    portfolioBeta = calculatePortfolioBeta(
        weights,
        betas,
    )
    systematicCovariance = calculateSystematicCovariance(betas)
    portfolioSystematicVariance = calculatePortfolioSystematicVariance(weights, betas)
    specificCovariance = calculateSpecificCovariance(specificVolatility)
    portfolioSpecificVariance = calculatePortfolioSpecificVariance(
        weights, specificVolatility
    )
    totalCovariance = calculateTotalCovariance(systematicCovariance, specificCovariance)
    portfolioVariance = calculatePortfolioVariance(
        portfolioSystematicVariance, portfolioSpecificVariance
    )

    return (
        portfolioBeta,
        specificCovariance,
        systematicCovariance,
        portfolioSpecificVariance,
        portfolioSystematicVariance,
        totalCovariance,
        portfolioVariance,
    )
