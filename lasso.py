import pandas as pd
import sklearn.linear_model
import sklearn.metrics
import numpy as np

class StandardLasso():
    #lasso regression with normalized variables
    #to regularize consistently along with optional non-zero priors
    def __init__(self, alpha=None, prior=None, fit_intercept=True, intercept_prior=None, demean_cols=None):
        self.alpha = alpha
        self.prior = prior
        self.fit_intercept = fit_intercept
        self.intercept_prior = intercept_prior

        #demean variables before doing anything else. any intercept output will be for a demeaned version of these
        #use case: starting with a regression of y = a + b*x1
        #with some intuition/priors about a and b
        #but then want to throw in other possible predictors x2, x3, x4
        #if you don't demean x2,x3,x4 first then it'll throw off the regression intercept and you'd need to
        #recalculate the prior
        self.demean_cols = demean_cols
    def initialize(self, X):
        params = {}
        if self.fit_intercept == False:
            params["fit_intercept"] = False
        elif self.fit_intercept == True and self.intercept_prior is None:
            #regular behavior
            pass
        elif self.fit_intercept == True and self.intercept_prior is not None:
            #handle this by adding a column of ones to the X var
            params["fit_intercept"] = False
        else:
            raise "Invalid arguments"
        #scale internal alpha inversely with number of rows to compensate for
        #sklearn's odd objective function which scales down error with the
        #number of datapoints
        #https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.Lasso.html
        #unlike the ridge objective which looks more standard?
        #https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.Ridge.html
        alpha = self.alpha / (2*len(X))
        self.mdl = sklearn.linear_model.Lasso(alpha=alpha, **params)
    def scale(self, X, fit=False):
        #scale all but the ones column, which should pass through
        cols_to_transform = [x for x in X.columns if x != "__ones"]
        if fit:
            if self.demean_cols:
                self.demean = sklearn.preprocessing.StandardScaler(with_std=False)
                self.demean.fit(X[self.demean_cols])
            self.scaler = sklearn.preprocessing.StandardScaler(with_mean=False)
            self.scaler.fit(X[cols_to_transform])
            #self.mean_x = self.scaler.mean_ #TODO: testing without mean, remove if this works
            self.scale_x = self.scaler.scale_
        if not self.scaler:
            raise Exception("must run with fit=True before running with fit=False")

        new_X = X.copy()
        if self.demean_cols:
            new_X[self.demean_cols] = self.demean.transform(new_X[self.demean_cols])
        new_X[cols_to_transform] = self.scaler.transform(new_X[cols_to_transform])
        return new_X
    def generate_ones(self, X):
        if self.intercept_prior is not None:
            #add column of ones
            ones = X.apply(lambda x: 1, axis=1)
            ones.name = "__ones"
            X = pd.concat([ones,X],axis=1)
        return X
    def drop_ones(self, X):
        X = X[[c for c in X.columns if c != "__ones"]]
        return X
    def fit(self, X, y, demean_cols=None, sample_weight=None):
        # print("testing123--------")
        # print("std")
        # print(y.std())
        # print("mean")
        # print(y.mean())
        self.initialize(X)
        #don't mutate the y variable that's passed in
        new_y = y - np.sum(self.prior * X, axis=1)
        if self.intercept_prior is not None:
            new_y -= self.intercept_prior
        self.scale_y = new_y.std()
        new_y /= self.scale_y
        params = {}
        if sample_weight is not None:
            params["sample_weight"] = sample_weight
        X = self.generate_ones(X)
        X = self.scale(X, fit=True)
        # print("fitting")
        # print("x")
        # print(X)
        # print(np.isnan(X))
        # print("y")
        # print(new_y)
        self.mdl.fit(X, new_y, **params)
    def predict(self, X):
        #don't mutate the passed in X variable
        new_X = self.generate_ones(X)
        new_X = self.scale(new_X)
        # print("step 0")
        # print(new_X)
        y = self.mdl.predict(new_X)
        # print("coeff")
        # print(self.mdl.coef_)
        # print(self.coeffs())
        # print("pred")
        # print(y)
        y *= self.scale_y
        # print("step 1")
        # print(y)
        if self.intercept_prior is not None:
            y += self.intercept_prior
        # print("step 2")
        # print(y)
        y += np.sum(self.prior * X, axis=1)
        # print("step 3")
        # print(y)
        return y
    def score(self, X, y, sample_weight=None):
        y_pred = self.predict(X)
        # print("score test")
        # print(X)
        # print("raw y")
        # print(y)
        # print("pred y")
        # print(y_pred)
        # print("error y")
        # print(y-y_pred)
        # print("error sq")
        # print((y-y_pred)**2)
        # print("weights")
        # print(sample_weight)
        # print("squares", np.average(y**2, axis=0,weights=sample_weight))
        # print("squares two", np.average((y-y_pred)**2, axis=0,weights=sample_weight))
        return sklearn.metrics.r2_score(y, y_pred, sample_weight=sample_weight)
    def normed_coeffs(self):
        if self.intercept_prior is not None:
            return list((self.mdl.coef_))[1:]
        else:
            return list((self.mdl.coef_))
    def normed_intercept(self):
        if self.intercept_prior is not None:
            return list((self.mdl.coef_))[0]
        else:
            return list([self.mdl.intercept_])[0]
    def coeffs(self):
        normed_coeffs = self.normed_coeffs()
        #means = list(self.mean_x) #TODO: testing without means, remove if this works
        sds = list(self.scale_x)
        raw_coeffs = [(self.scale_y * coef / sd) + prior for (coef, sd, prior) in zip(normed_coeffs, sds, self.prior)]
        # print("test")
        # print("y scale", self.scale_y)
        # print("normed:", normed_coeffs)
        # print("means:", means)
        # print("sds:", sds)
        # print("raw:", raw_coeffs)

        if self.fit_intercept and self.intercept_prior is None:
            #TODO: update for the new schema?
            normed_intercept = self.normed_intercept()
            raw_intercept = normed_intercept - sum([coef * mean * self.scale_y for (coef,mean) in zip(raw_coeffs, means)])
            raw_coeffs = [raw_intercept] + raw_coeffs
        elif self.fit_intercept and self.intercept_prior is not None:
            normed_intercept = self.normed_intercept()
            raw_intercept = (normed_intercept * self.scale_y + self.intercept_prior)
            raw_coeffs = [raw_intercept] + raw_coeffs # - sum([coef * mean * self.scale_y / sd for (coef,mean,sd) in zip(normed_coeffs,means,sds)]) #TODO: testing without mean, remove permanently if this works

            # print("intercept calculation")
            # print(normed_intercept, self.scale_y, self.intercept_prior, self.prior, means, normed_coeffs, raw_coeffs)
            # print("post")
            #(y - xi*pi - yp) / sy = a + bi * (xi-ui) / si
            #y - yp - xi*pi = a*sy - bi*sy*ui/si + bi*sy*xi/si
            #y = a*sy + yp - bi*sy*ui/si + xi*pi + bi*sy*xi/si
            #y = (a*sy + yp - bi*sy*ui/si) + (pi + bi*sy/si) * xi
            #want: a = 0, bi = 0 -> y = yp + xi*pi check

            #thinking:
            #(y - xi*pi) / sy = bi * xi / si

        return raw_coeffs
