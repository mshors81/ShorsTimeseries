# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Install Package:           'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'

Timeseries.Tensor_RescaleColumns <- function(Dataset,Y,Xs,Lookback,Mirror = F,Scale = c(0,1),ReferenceDataset = NULL,DateColumn = 'ds'){
  returnlist <- list()

  Dataset_Copy <- copy(na.omit(Dataset))
  Dataset_Copy <- Dataset_Copy[order(-ds)]
  Dataset_Copy[,Date_ID := .I]
  Dataset_Copy_original <- copy(Dataset_Copy)


  setnames(Dataset_Copy,Y,'Y')
  setnames(Dataset_Copy,DateColumn,'ds')

  Xs <- Xs[Xs != Y]

  if (!is.null(Scale)){
    for (i in c(Xs)){
      if(is.null(ReferenceDataset)){
        ReferenceDataset <- Dataset_Copy
      }

      Dataset_Copy[, c(i) := scales::rescale(get(i),to = Scale,from = c(0,max(ReferenceDataset[,get(i)])))]
    }
  }

  DCastResults <- foreach(i = Xs)%do%{
    Timeseries.DcastColumn(Dataset = Dataset_Copy,Target_Col = i,Lookback)
  }

  DCastResults <- lapply(DCastResults,function(X)(X[,.SD,.SDcols = !c('Date_ID')]))




  Stop <- nrow((DCastResults[[1]])) - (nrow((DCastResults[[1]])) -  nrow(na.omit(DCastResults[[1]])))

  X_Array <- abind::abind(DCastResults,along = 3)
  Y_Vector <- Dataset_Copy$Y


  returnlist$DF_Original <- copy(Dataset_Copy_original[1:Stop,])

  returnlist$DF <- copy(Dataset_Copy[1:Stop,])
  returnlist$X <- X_Array[1:Stop,,,drop = F]
  returnlist$Y <- Y_Vector[1:Stop]

  return(returnlist)

}

Timeseries.Tensor_Data <- function(Dataset,Y,Xs,Lookback,Mirror = F,DateColumn = 'ds'){
  returnlist <- list()

  Dataset_Copy <- copy(na.omit(Dataset))
  Dataset_Copy <- Dataset_Copy[order(-ds)]
  Dataset_Copy[,Date_ID := .I]
  Dataset_Copy_original <- copy(Dataset_Copy)


  setnames(Dataset_Copy,Y,'Y')
  setnames(Dataset_Copy,DateColumn,'ds')

  Xs <- Xs[Xs != Y]


  DCastResults <- foreach(i = Xs)%do%{
    Timeseries.DcastColumn(Dataset = Dataset_Copy,Target_Col = i,Lookback)
  }

  DCastResults <- lapply(DCastResults,function(X)(X[,.SD,.SDcols = !c('Date_ID')]))


  Stop <- nrow((DCastResults[[1]])) - (nrow((DCastResults[[1]])) -  nrow(na.omit(DCastResults[[1]])))

  X_Array <- abind::abind(DCastResults,along = 3)
  Y_Vector <- Dataset_Copy$Y


  returnlist$DF_Original <- copy(Dataset_Copy_original[1:Stop,])

  returnlist$DF <- copy(Dataset_Copy[1:Stop,])
  returnlist$X <- X_Array[1:Stop,,,drop = F]
  returnlist$Y <- Y_Vector[1:Stop]

  return(returnlist)

}

Timeseries.Tensor_DataRescale  <- function(Dataset,DatasetMin,DatasetMax,Y,Xs,Lookback,Mirror = F,DateColumn = 'ds'){
  returnlist <- list()

  Dataset_Copy <- copy(na.omit(Dataset))
  Dataset_Copy <- Dataset_Copy[order(-ds)]
  Dataset_Copy[,Date_ID := .I]
  Dataset_Copy_original <- copy(Dataset_Copy)


  setnames(Dataset_Copy,Y,'Y')
  setnames(Dataset_Copy,DateColumn,'ds')

  Xs <- Xs[Xs != Y]

  DCastResults <- foreach(i = Xs)%do%{
    Timeseries.DcastColumn(Dataset = Dataset_Copy,Target_Col = i,Lookback)
  }


  DatasetMin <- DatasetMin[ds %in% Dataset_Copy$ds][order(-ds)]
  DatasetMax <- DatasetMax[ds %in% Dataset_Copy$ds][order(-ds)]
  MinScale = 0
  MaxScale = 1
  iX <- Xs[1]

  ReScaled_List <- foreach (iX = Xs)%do%{
    Position <- which(Xs == iX)
    Min = DatasetMin[,.SD,.SDcols = c('ds',iX)]
    setnames(Min,c('ds','Min'))

    Max = DatasetMax[,.SD,.SDcols = c('ds',iX)]
    setnames(Max,c('ds','Max'))

    Matix <- DCastResults[[Position]]

    ReScaleMatrix <- foreach( i = rev(as.character(0:Lookback)),.combine = cbind)%do%{

      (MaxScale - MinScale)*((Matix[[i]] - Min$Min)/(Max$Max -  Min$Min))+MinScale
    }

    ReScaleMatrix <- data.table(ReScaleMatrix)
    colnames(ReScaleMatrix) <- rev(as.character(0:Lookback))
    return(ReScaleMatrix)
  }




  DCastResults <- ReScaled_List


  Stop <- nrow((DCastResults[[1]])) - (nrow((DCastResults[[1]])) -  nrow(na.omit(DCastResults[[1]])))

  X_Array <- abind::abind(DCastResults,along = 3)
  Y_Vector <- Dataset_Copy$Y

  returnlist$DF_Original <- copy(Dataset_Copy_original[1:Stop,])

  returnlist$DF <- copy(Dataset_Copy[1:Stop,])
  returnlist$X <- X_Array[1:Stop,,,drop = F]
  returnlist$Y <- Y_Vector[1:Stop]

  return(returnlist)

}

Timeseries.DcastColumn <- function(Dataset,Target_Col,Lookback){
  Dataset_Copy <- copy(Dataset)
  setnames(Dataset_Copy,Target_Col,'Adjusted_Col')
  Dataset_Copy <- Dataset_Copy[,.(Date_ID,Adjusted_Col)][order(-as.numeric(Date_ID))]
  for (i in as.character(seq(0,Lookback,1))){
    Dataset_Copy[,c(as.character(Lookback - (Lookback - as.numeric(i)))) := shift(Adjusted_Col,n = as.numeric(i),type = 'lag')]
    Dataset_Copy[order(-as.numeric(Date_ID))]
  }
  Dataset_Copy[,Adjusted_Col := NULL]
  Dataset_Copy <- Dataset_Copy[,.SD,.SDcols = c('Date_ID',as.character(rev(seq(0,Lookback,1))))]
  Dataset_Copy[order(as.numeric(Date_ID))]
}

Timeseries.GetModel <- function(ODBCConnection,ModelName){
  ModelDefinition <- SQLiteDB.GetData(ODBCConnection,TableName = 'Tensorflow_Models',PredicateList = list(ModelName = ModelName))

  return(ModelDefinition)
}

Timeseries.SQLiteDB.GenerateModelLayers <- function(ODBCConnection,ModelID = NULL,Output_Activation){

  if(is.null(ModelID)){
    MaxModelsOptions <- SQLiteDB.Query(ODBCConnection,TableName = 'Model_ModelOptions',Columns = ('max(ID)'))
    Model <- sample(1:MaxModelsOptions$`max(ID)`,1)
    PickedModel <- SQLiteDB.Query(ODBCConnection,TableName = 'Model_ModelOptions',PredicateList = list(ID = Model))
  }else{
    PickedModel <- SQLiteDB.Query(ODBCConnection,TableName = 'Model_ModelOptions',PredicateList = list(ID = ModelID))
  }


  LayerNum <- PickedModel$Layers
  Layers <- PickedModel$DBLayers
  Layers <- strsplit(Layers,",")[[1]]

  i <- 8
  LayerList <- foreach(i = Layers)%do%{
    LayerOptions <-  SQLiteDB.Query(ODBCConnection,TableName = 'Model_LayerOptions',PredicateList = list(ID = i))
    LayerID <- LayerOptions$ID
    LayerOptions[,":=" (ID = NULL, UploadJob = NULL)]

    if(i == Layers[1] & length(Layers) > 1){
      LayerOptions[,return_sequence := 'T']
    }

    as.list(LayerOptions)
  }

  if(length(LayerList) == 2){
    LayerList[[2]]$return_sequence <- 'F'
  }




  LayerList[[length(LayerList) + 1]] <- list(BiDirectional = F,IncludeModel =T,Layer_Type = 'layer_dense',Units = '1',activation = Output_Activation)


  Layers <- lapply(LayerList,function(X)(do.call(Timeseries.CreateLayer,X)))


  names(Layers) <- lapply(LayerList,function(X)(X$Layer_Type))

  return(list(ModelID = PickedModel,Model_List = Layers))

}

Timeseries.BuildModel <- function(Layers,InputShape = dim(SampleTrainingSet)[2:3], SampleTrainingSet = NULL){

  if(!is.null(SampleTrainingSet)){
    InputShape <- dim(SampleTrainingSet)[2:3]
  }

  InputLayerLoc <- min(which(names(Layers) != 'layer_Init'))

  InputLayer <- Layers[[InputLayerLoc]]
  InputShape <- paste('input_shape = c(',paste(InputShape,collapse = ","),'),')
  Layers[[InputLayerLoc]] <- gsub('model,',paste0('model, ', InputShape),InputLayer)

  model <- keras_model_sequential()

  for(iLine in Layers){
    eval(parse(text = iLine))
  }
  return(model)
}

Timeseries.CreateLayer<- function(IncludeModel,BiDirectional = F,Layer_Type = c('layer_Init','layer_dense','layer_gru','layer_lstm'),Units,activation = c('elu','relu'),...){

  if(Layer_Type == 'layer_Init'){
    return(Layer_Type = 'model <- keras_model_sequential()')
  }

  Additionl_Arg <- list(...)

  if(BiDirectional){
    layer <- Timeseries.CreateBaseLayer(IncludeModel =F,Layer_Type = Layer_Type,Units = Units,activation = activation,Additional_Arg = Additionl_Arg)
    return(Layer_Type = paste0("bidirectional(model, ",layer,")"))
  }else{
    return(Layer_Type = Timeseries.CreateBaseLayer(IncludeModel =IncludeModel,Layer_Type = Layer_Type,Units = Units,activation = activation,Additional_Arg = Additionl_Arg))
  }

}

Timeseries.CreateBaseLayer <- function(IncludeModel,Layer_Type = c('layer_dense','layer_gru','layer_lstm'),Units,activation = c('elu','relu'),Additional_Arg = list()){
  Additionl_Arg_names <- names(Additional_Arg)

  ArgList <- list()
  ArgList$layer_gru <- c("recurrent_dropout","dropout")
  ArgList$layer_lstm <- c("recurrent_dropout","dropout")
  ArgList$layer_dense <- c()


  LayerArgs <- ArgList[[Layer_Type]]

  print(Additional_Arg)

  if(!all(LayerArgs %in% Additionl_Arg_names)){
    stop(paste('Missing Required Argements missing:',paste(LayerArgs[!LayerArgs %in% Additionl_Arg_names],collapse=",")))
  }

  if(length(Additional_Arg) > 0){
    Additionl_Args <- paste0(', ',paste(paste(names(unlist(Additional_Arg)),unlist(Additional_Arg),sep = "="),collapse=", "))
  }else{
    Additionl_Args <- ''
  }

  paste0("",Layer_Type,"(",ifelse(IncludeModel,'model, ',''),"units = ",Units,",activation= '",activation,"'",Additionl_Args,")")
}

Timeseries.Model_JSONCreate <- function(ODBCConnection,ModelID,InputShape =  paste(dim(TensorDataset$X)[2:3],collapse = "_"),Output_Activation = 'sigmoid',POST = T){
  JSONModel <- SQLiteDB.GetData(ODBCConnection,TableName = 'GetModelJSONFunctions',PredicateList = list(ModelID = ModelID, Output_Activation =Output_Activation,InputShape = InputShape))$Data[[1]]

  if(is.null(JSONModel)){
    ModelLayers <- Timeseries.SQLiteDB.GenerateModelLayers(ODBCConnection,ModelID  = ModelID,Output_Activation = Output_Activation)

    Model <-  Timeseries.BuildModel(Layers = ModelLayers$Model_List,InputShape = gsub("_",",",InputShape))
    JSONModel <- keras::model_to_json(Model)

    if(POST){
      SQLiteDB.PostData(ODBCConnection,TableName = 'GetModelJSONFunctions',Data = JSONModel,PredicateList = list(ModelID = ModelID, Output_Activation =Output_Activation,InputShape = InputShape),Overwrite = F)
      JSONModel <- SQLiteDB.GetData(ODBCConnection,TableName = 'GetModelJSONFunctions',PredicateList = list(ModelID = ModelID, Output_Activation =Output_Activation,InputShape = InputShape))$Data[[1]]
    }
  }
  return(JSONModel)
}

Timeseries.WriteWeights <- function(ModelName,epoch,Loss,Validationloss,Validationmetric,Weights,StrategyName,StrategyArgumentName,AnswerKeyName){
  warning('New Best')

  Weights_Json <- jsonlite::serializeJSON(Weights)
  Results <- data.table(Name = ModelName,Epoch = epoch,Loss,Weights = Weights_Json,DateTime = Sys.time())
  Results[,Val_metric := Validationmetric]
  Results[,Val_loss := Validationloss]
  Results[,StrategyName := StrategyName]
  Results[,StrategyArgumentName := StrategyArgumentName]
  Results[,AnswerKeyName := AnswerKeyName]

  FileName <- paste0(paste(ModelName,StrategyArgumentName,StrategyName,AnswerKeyName,sep="|"),'.txt')

  dir.create(file.path(getwd(),'Weights'),showWarnings  = F)

  data.table::fwrite(Results,file.path(getwd(),'Weights',FileName),append = F,sep="|",quote = F)

}

Timeseries.ReadWeightFiles <- function(Folder = 'Weights',Directory = NULL,IncludeWeight = F){

  if(is.null(Directory)){
    Directory <- getwd()
  }

  Location = file.path(Directory,Folder)

  Files = list.files(Location,include.dirs = F)
  Files <- Files[tolower(Files) %like% '.txt']

  Weight_List <- foreach(iFile = Files)%do%{
    DF <- data.table::fread(file.path(Location,iFile),sep="|")

    DF[,Location := file.path(Location,iFile)]
    if(!IncludeWeight){
      DF[,Weights := NULL]

    }
    DF
  }



  return(rbindlist(Weight_List,fill = T)[order(-Val_metric)])
}

Timeseries.UploadWeightFiles <- function(ODBCConnection,Weights){

  MoveDir_List <- foreach(iWeight = 1:nrow(Weights))%do%{

    NewWeights <- Weights[iWeight]

    CurrentWeights <- RSQLite.Query('Tensorflow_Weights',PredicateList = list(Name = NewWeights$Name))

    Location <- NewWeights$Location


    NewWeights[, c("ModelID", "GeneratorName",'ArgName') := tstrsplit(Name, "-", fixed=TRUE)]

    MoveDir <- file.path(dirname(Location),'Processed')
    dir.create(MoveDir,showWarnings = F)

    file.rename(Location, file.path(MoveDir,basename(Location)))

    if(nrow(CurrentWeights) == 0){
      RSQLite.Upload(TableName = 'Tensorflow_Weights',TableData = NewWeights[,.SD,.SDcols = c('Name','Epoch','Loss','Weights','DateTime')])
    }else{
      if(lubridate::force_tz(lubridate::as_datetime(CurrentWeights$DateTime),tz = 'UTC') < NewWeights$DateTime){
        RSQLite.Delete('Tensorflow_Weights',PredicateList = list(Name = NewWeights$Name))
        RSQLite.Upload(TableName = 'Tensorflow_Weights',TableData = NewWeights[,.SD,.SDcols = c('Name',"ModelID", "GeneratorName",'ArgName','Epoch','Loss','Weights','DateTime')])
      }
    }
    return(file.path(MoveDir,basename(Location)))
  }

  return(MoveDir_List)

}

Timeseries.LoadWeights <- function(ODBCConnection,Model,PredicateList = list(Strategy,StrategyArguments,AnswerKeyName,ModelID,DatasetSource,DatasetSymbol)){

  Weights <- SQLiteDB.GetData(ODBCConnection,TableName = 'Model_Weights',PredicateList = PredicateList)$Data[[1]]

  if(length(Weights) > 0){
    print('Weights Loaded from Table...')
    Weights <- jsonlite::unserializeJSON(Weights)
    keras::set_weights(Model,Weights)
  }
  return(Model)
}

Timeseries.ModelStrategy <- function(ODBCConnection,StrategyArgument,ModelID = NULL,Process = c('Train','Predict','Load'),Train = T){

  StrategyName <- StrategyArgument$Strategy$StrategyName
  StrategyArgumentName <- StrategyArgument$Strategy$StrategyArgumentName

  DatasetSource <- StrategyArgument$TrainingDatasetInputs$DatasetSource
  DatasetSymbol <- StrategyArgument$TrainingDatasetInputs$DatasetSymbol

  AnswerKeyName <- StrategyArgument$AnswerKeyInputs$AnswerKeyName
  AnswerKeyFunction <- StrategyArgument$AnswerKeyInputs$AnswerKeyFunction

  if(is.null(ModelID)){
    stop('Please specify ModelID')
  }else{
    print(paste(Process, ModelID,sep="-"))
  }

  if(Process == 'Train'){
    print('Running Callback...')

    Timeseries.SaveWeightsCallBack(keras$callbacks$Callback) %py_class% {
      "Stop training when the loss is at its min, i.e. the loss stops decreasing.

  Arguments:
      patience: Number of epochs to wait after min has been hit. After this
        number of no improvement, training stops.
  "

      initialize <- function(patience = 0) {
        # call keras$callbacks$Callback$__init__(), so it can setup `self`
        super$initialize()
        self$patience <- patience
        # best_weights to store the weights at which the minimum loss occurs.
        self$best_weights <- NULL
      }

      on_train_begin <- function(logs = NULL) {
        # The number of epoch it has waited when loss is no longer minimum.
        self$wait <- 0
        # The epoch the training stops at.
        self$stopped_epoch <- 0
        # Initialize the best as infinity.
        self$best <- Inf

        #  RSQLite.Upload(TableName = 'Model_Starts',TableData = data.table(ModelName,DT = Sys.time()))


      }

      on_epoch_end <- function(epoch, logs = NULL) {
        current <- logs$loss

        if (current < self$best) {
          self$best <- current
          self$wait <- 0
          # Record the best weights if current results is better (less).
          self$best_weights <- self$model$get_weights()

          Validationmetric <- logs$val_accuracy
          Validationloss <- logs$val_loss


          Timeseries.WriteWeights(ModelName = ModelID,
                                  epoch = epoch,
                                  Loss = current,
                                  Validationmetric = Validationmetric,
                                  Validationloss = Validationloss,
                                  Weights = self$best_weights,
                                  StrategyName = StrategyName,
                                  StrategyArgumentName = StrategyArgumentName,
                                  AnswerKeyName = AnswerKeyName)




        } else {
          self$wait %<>% `+`(1)
          if (self$wait >= self$patience) {
            self$stopped_epoch <- epoch
            self$model$stop_training <- TRUE
            cat("Restoring model weights from the end of the best epoch.\n")
            self$model$set_weights(self$best_weights)
          }
        }
      }

      on_train_end <- function(logs = NULL){
        if (self$stopped_epoch > 0)
          cat(sprintf("Epoch %05d: early stopping\n", self$stopped_epoch + 1))
        #   RSQLite.Upload(Sys.getenv('ODBCFilePath'),TableName = 'Model_Ends',TableData = data.table(ModelName,DT = Sys.time()))
      }


      layer_dense
    }
  }

  print(paste('Getting Dataset...'))

  #Dataset Function
  DataSetFunction <- SQLiteDB.GetData(ODBCConnection,TableName = 'GetDatasetFunctions',PredicateList = list(Source = DatasetSource))$Data[[1]]
  Dataset <- DataSetFunction(ODBCConnection,DatasetSymbol)

  #ETL
  print('Get Dataset...')
  SQLiteDB.Query(ODBCConnection,'ETLFunctions')
  ETLFunction <- SQLiteDB.GetData(ODBCConnection,TableName = 'ETLFunctions',PredicateList = list(Dataset = DatasetSource,Strategy = StrategyName))$Data[[1]]
  DatasetETL <- ETLFunction(Dataset)

  #Get Answers
  print('Get Answerkey...')
  AnswerKey <- SQLiteDB.GetData(ODBCConnection,
                                TableName = 'TrainingDatasetAnswers',
                                PredicateList = list(
                                  AnswerKeyFunction = AnswerKeyFunction,
                                  AnswerKeyName =  AnswerKeyName,
                                  Source = DatasetSource,
                                  SourceName = DatasetSymbol
                                ))$Data[[1]]

  print('Get StrategyFunction...')

  #Strategy
  StrategyFunction <- SQLiteDB.GetData(ODBCConnection,TableName = 'StrategyFunctions',PredicateList = list(Strategy = StrategyName))$Data[[1]]

  StrategyInputArguments <- StrategyArgument[names(StrategyArgument)[names(StrategyArgument) %in% formalArgs(StrategyFunction)]]
  StrategyInputArguments$DatasetETL = DatasetETL
  StrategyInputArguments$AnswerKey = AnswerKey
  StrategyInputArguments$Train = Train

  #Make Tensor
  set.seed(100)
  print('Running TensorDataset...')

  TensorDataset <- do.call(StrategyFunction,StrategyInputArguments)

  #Get Model
  print(paste('Create Model-',ModelID,'...'))
  InputShape <- paste(dim(TensorDataset$X)[2:3],collapse = "_")
  JSONModel <- Timeseries.Model_JSONCreate(ODBCConnection,ModelID,InputShape = InputShape,Output_Activation  = 'sigmoid',POST = F)

  #Create Model from JSON
  Model <- keras::model_from_json(JSONModel)
  print('Create Compile...')

  #Compile Model
  Model %>% compile(
    loss = StrategyArgument$ModelSetupInputs$LossFunction,
    optimizer = StrategyArgument$ModelSetupInputs$OptimizerFunction,
    metrics = c(StrategyArgument$ModelSetupInputs$Metrics))
  print('Checking Weights')

  WeightPredicateList <- list(
    StrategyName =StrategyName,
    StrategyArgumentName = StrategyArgumentName,
    AnswerKeyName =AnswerKeyName,
    Name = ModelID)

  Model <- Timeseries.LoadWeights(ODBCConnection,Model,PredicateList = WeightPredicateList)

  #Train Model
  X_Train <- TensorDataset$X[TensorDataset$TrainingSample,,,drop = F]
  Y_Train <- TensorDataset$Y[TensorDataset$TrainingSample,drop = F]

  X_Test <- TensorDataset$X[TensorDataset$TestSample,,,drop = F]
  Y_Test <- TensorDataset$Y[TensorDataset$TestSample,drop = F]


  if(Process == "Load"){
    print('LoadOnly...')

    returnlist <- list()
    returnlist$DataSetFunction <- DataSetFunction
    returnlist$Dataset <- Dataset
    returnlist$ETLFunction <- ETLFunction
    returnlist$DatasetETL <- DatasetETL
    returnlist$AnswerKey <- AnswerKey
    returnlist$StrategyFunction <- StrategyFunction
    returnlist$StrategyInputArguments <- StrategyInputArguments
    returnlist$JSONModel <- JSONModel
    returnlist$Model <- Model
    returnlist$TensorDataset <- TensorDataset
    return(returnlist)

  }else if(Process == 'Train'){
    print('Running Model...')

    history <- Model %>% fit(X_Train,Y_Train,
                             batch_size = StrategyArgument$TrainingInputs$batch_size,
                             epochs = StrategyArgument$TrainingInputs$epochs,verbose = T,view_metrics = F
                             ,validation_data = list(X_Test,Y_Test),callbacks = list(Timeseries.SaveWeightsCallBack(patience = StrategyArgument$TrainingInputs$patience)))

    return(history)
  }else if(Process == 'Predict'){

    Predicted <-   predict(Model,TensorDataset$X,batch_size = 1000)

    ReturnDF <- copy(TensorDataset$DF_Original)
    ReturnDF$Predicted <-  Predicted
    ReturnDF[AnswerKey,Actual := Y, on = .(ds)]

    return(ReturnDF)

  }






}

Timeseries.ProcessWeightFiles <- function(ODBCConnection,Location,OverrideAll = F){

  NewWeights <- Timeseries.ReadWeightFiles(Directory = Location,IncludeWeight = T )

  #Get Current Weights

  foreach(i = 1:nrow(NewWeights))%do%{
    NewWeight <- NewWeights[i]

    WritePredicates <- NewWeight[,.SD,.SDcols = c( 'Name', 'Epoch','Loss','Val_metric','Val_loss','DateTime','StrategyName', 'StrategyArgumentName','AnswerKeyName')]
    ReadPredicates <- NewWeight[,.SD,.SDcols = c( 'Name','StrategyName', 'StrategyArgumentName','AnswerKeyName')]

    CurrentWeightValues <- SQLiteDB.Query(ODBCConnection,TableName = 'Model_Weights',PredicateList = ReadPredicates)
    NewWeightValues <-as.list(WritePredicates)

    OldLoss <- CurrentWeightValues$Loss
    NewLoss <- NewWeight$Loss


    if(OverrideAll){
      print('OverrideAll, Uploading...')


      SQLiteDB.DeleteData(ODBCConnection,TableName = 'Model_Weights',ReadPredicates)

      SQLiteDB.PostData(ODBCConnection,
                        TableName = 'Model_Weights',
                        Data = NewWeight$Weights,
                        PredicateList = as.list(NewWeightValues),
                        Overwrite = F)
    }else{


      if(length(OldLoss) == 0){
        print('New Loss, Uploading...')

        SQLiteDB.PostData(ODBCConnection,
                          TableName = 'Model_Weights',
                          Data = NewWeight$Weights,
                          PredicateList = as.list(NewWeightValues),
                          Overwrite = F)
      }else{
        if(NewLoss < OldLoss){
          print('Better Loss, Uploading...')


          SQLiteDB.DeleteData(ODBCConnection,TableName = 'Model_Weights',ReadPredicates)

          SQLiteDB.PostData(ODBCConnection,
                            TableName = 'Model_Weights',
                            Data = NewWeight$Weights,
                            PredicateList = as.list(NewWeightValues),
                            Overwrite = F)

        }else{
          print('No New Loss, Skip...')

        }
      }


    }





    FromLocation <- NewWeight$Location
    ToLocation <- gsub('/Weights/','/ProcessedWeights/',FromLocation)
    dir.create(dirname(ToLocation),showWarnings = F)
    file.rename(FromLocation, ToLocation)

  }



}

Timeseries.OpenTrainingCluster <- function(Available_GPUs){
  Timeseries.ResetGPU(Available_GPUs)
  Clusters <- length(Available_GPUs)
  cl <- Timeseries.OpenCluster(Clusters)
  foreach(iP= c(1:Clusters),.combine = rbind,.packages=c('data.table','zoo','foreach','keras','tensorflow','Thermimage','abind')) %dopar% {
    Sys.setenv("CUDA_VISIBLE_DEVICES" = as.character(Available_GPUs[(iP%%Clusters)+1]))

    k_clear_session()
    setDTthreads(2)
    getDTthreads()
  }
  cl
}

Timeseries.OpenCluster <- function(cores){
  cl <- makeCluster(cores, outfile="")
  registerDoSNOW(cl)
  cl
}

Timeseries.ResetGPU <- function(Cards){
  a <- system("nvidia-smi", intern = TRUE)
  a <- foreach( i = 1:length(a),.combine = rbind)%do%{
    if(a[i] %like% '/usr/' == T){
      a[i]
    }else{
    }
  }
  a <- data.table(a)
  if(nrow(a) > 0){
    a <- a[,.(str_squish(V1))]
    a[, c("A", "Card",'PID','D','E','Memory','G') := tstrsplit(V1, " ", fixed=TRUE)]
    Kill_PIDs <- a[Card %in% Cards]$PID
    for (i in Kill_PIDs){
      system(paste("kill -9 ",i,sep=" "))
    }
  }
}

Timeseries.BackTest <- function(.DF_Predict,StartingFunds = 10000,Split = c(.6,.8)){

  #Whatif Senerio
  DF_Predict <- copy(.DF_Predict)
  DF_X <- copy(DF_Predict[,.(ds,X,Prediction = Predict)])
  Dates <- as.character(sort(unique(as.POSIXct(DF_X$ds))))
  DF_X[,ds := as.character(ds)]

  setkey(DF_X,'ds')

  iStart <- 100
  Funds <- StartingFunds
  Position <- 0
  Proceeds <- 0
  Ledger <- list()

  PositionLevel <- 0

  AllocationFunction <- function(CurrentLevel,Afford){
    if(PositionLevel == 0){
      return(Afford)
    }else if(PositionLevel == 1){
      return(Afford)
    }else{
      return(0)
    }
  }

  i <- 1

  for (i in 1:(nrow(DF_X)-100)){

    # if(length(Funds)>1){
    #   print(i)
    # }

    if(i %% 1000 == 0){
      print(paste0(round((i / nrow(DF_X)) * 100,0),"%"))
    }

    iDF_1 <- DF_X[ds == Dates[iStart + i]]

    Prediction <- iDF_1$Prediction
    Price <- iDF_1$X

    if(Prediction > Split[[1]]){
      Afford <- Funds/Price
      Afford <- AllocationFunction(PositionLevel,Afford)
      Position <- Position + Afford
      Funds <- Funds - (Afford * Price)
      if(Afford >0){
        PositionLevel <- PositionLevel + 1
        iTS <- iDF_1
        iTS[,Action := 'Buy']
        iTS[,Position := ((Position * Price) + Funds)]
        Ledger[[paste(i)]] <- iTS
      }

    }else if(Prediction < Split[[2]]){
      Proceeds <- Position * Price
      Funds <- Funds + Proceeds
      Position <- 0
      PositionLevel <- 0
      if(Proceeds > 0){
        iTS <- iDF_1
        iTS[,Action := 'Sell']
        iTS[,Position := ((Position * Price) + Funds)]
        Ledger[[paste(i)]] <- iTS
      }
    }

  }

  Ledger_all <- rbindlist(Ledger)
  Ledger_all[,ID := .I]

  Ledger_all[,ModelID := unique(DF_Predict$ModelID)]
  Ledger_all[,Strategy  := unique(DF_Predict$Strategy )]
  Ledger_all[,StrategyArguments  := unique(DF_Predict$StrategyArguments )]
  Ledger_all[,Training := unique(DF_Predict$Training)]

  Ledger_all[,PositionLag := data.table::shift(Position,1)]
  Ledger_all[Action == 'Sell',Proceed := Position - PositionLag]





  return(Ledger_all)

}

Timeseries.GetNewParameters <- function(X,OptiTarget,parameters,ParameterList,clusters){

  Bayes_DF <- X[,.SD,.SDcols = c(parameters,OptiTarget)]
  setnames(Bayes_DF,OptiTarget,'Target')

  print(Bayes_DF)

  y_best <- Bayes_DF[Target > 0,min(Target)]

  print(y_best)

  CharacterFactor <- colnames(cbind(dplyr::select_if(Bayes_DF, is.factor),dplyr::select_if(Bayes_DF, is.character)))

  for (i in CharacterFactor){
    Bayes_DF[,c(i) := as.factor(as.character(get(i)))]
  }

  Bayes_DF[,Target := as.numeric(as.character(Target))]
  Bayes_DF <- copy(na.omit(Bayes_DF[Target > 0]))

  TrainingData <- data.frame(janitor::remove_constant(Bayes_DF[Target > 0,.SD,.SDcols = c(parameters,'Target')]))

  print(TrainingData)
  model <-
    stan_glm(
      Target ~ .,
      family = gaussian(),
      data = TrainingData,
      priors = NULL,
      prior_intercept = NULL,chains = 5, cores = 5, iter = 4000
    )

  New_Parameters <- copy(X[get(OptiTarget) == 0])

  PredictSample <- data.table(dplyr::sample_n(New_Parameters,5000))

  Predicts <- posterior_predict(model,newdata =  PredictSample[,.SD,],draws = 1000,re.form = NA)

  Predicts <- data.table(Predicts)
  PredictSample[,SD := data.table(t(Predicts[, lapply(.SD, sd, na.rm=TRUE)]))]
  PredictSample[,Mean := data.table(t(Predicts[, lapply(.SD, mean, na.rm=TRUE)]))]

  PredictSample[,PI := Timeseries.probability_improvement(Mean,SD,y_best),by = .(ID)]
  PredictSample[,EI := Timeseries.expected_improvement(Mean,SD,y_best),by = .(ID)]

  KMean_DF <- PredictSample[,.SD,.SDcol = parameters]

  Cluster_Vector <- sample(1:100,nrow(KMean_DF),replace = T)

  PredictSample$cluster <- Cluster_Vector


  PredictSample[,EIRank := frank(-EI),by = cluster]
  PredictSample[,PIRank := frank(-PI),by = cluster]

  return(PredictSample[EIRank == 1 | PIRank ==1])
}

Timeseries.expected_improvement <- function(m, s,y_best) {
  if (s == 0) return(0)
  gamma <- (y_best - m) / s
  phi <- pnorm(gamma)
  return(s * (gamma * phi + dnorm(gamma)))
}

Timeseries.probability_improvement <- function(m, s,y_best) {
  if (s == 0) return(0)
  else {
    poi <- pnorm((y_best - m) / s)
    # poi <- 1 - poi (if maximizing)
    return(poi)
  }
}

Timeseries.GetModelSuggestions <- function(ODBCConnection,Target = 'Loss',Strategy = 'ClosePriceShiftSpread',StrategyArguments = 'S1_0_15_10_F'){

  ModelNew <-SQLiteDB.Query(ODBCConnection,TableName = 'Model_Menu',Top = 10000,Random = T)
  ScoredModels <- data.table(odbc::dbGetQuery(ODBCConnection,paste0("select a.*,b.Loss,b.strategy,b.strategyarguments from Model_Menu a left join Model_Weights b on a.ID = b.name where b.loss is not null and Strategy = '",Strategy,"' and StrategyArguments = '",StrategyArguments,"'")))

  Models <- rbind(ModelNew,ScoredModels,fill = T)

  colnames(Models) <- make.names(colnames(Models))

  OptimizationListsReasults <- copy(Models)
  OptimizationListsReasults[,":=" (UploadJob = NULL,Layer1   = NULL, Layer2 = NULL,DBLayers  = NULL,Strategy  = NULL, StrategyArguments  = NULL)]

  for (i in colnames(OptimizationListsReasults)){
    Col <- OptimizationListsReasults[[i]]
    ColClass <- class(OptimizationListsReasults[[i]])
    if(ColClass[1] %in% c('factor','character')){
      Col[is.na(Col)] <- 'None'
    }else{
      Col[is.na(Col)] <- 0
    }
    OptimizationListsReasults[[i]] <- Col
  }

  parameters <- colnames(OptimizationListsReasults)

  parameters <- parameters[parameters != Target]

  parameters <- parameters[parameters != 'ID']

  ParameterList <- foreach(i = parameters)%do%{
    unique(OptimizationListsReasults[[i]])
  }
  names(ParameterList) <- parameters

  return(Timeseries.GetNewParameters(X = copy(OptimizationListsReasults),OptiTarget = Target,parameters,ParameterList,clusters = 100))

}
